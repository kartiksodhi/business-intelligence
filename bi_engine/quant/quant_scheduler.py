"""
Quant module scheduler jobs.

Integrates with the existing APScheduler instance in routing/scheduler.py.
Call add_quant_jobs(scheduler, pool) from routing/scheduler.py:create_scheduler().

Jobs:
  - bhav_copy_fetch      daily 4:00 PM IST (after market close)
  - factor_compute       daily 5:00 PM IST (after prices loaded)
  - signal_returns_fill  daily 5:30 PM IST (fill elapsed forward windows)
  - portfolio_rebalance  weekly Monday 9:00 AM IST (before market open)
  - cin_bridge_refresh   monthly 1st of month 2:00 AM IST
"""

from __future__ import annotations

import logging
from datetime import date, timezone, timedelta
import pytz

import asyncpg
from apscheduler.schedulers.asyncio import AsyncIOScheduler

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")


def add_quant_jobs(scheduler: AsyncIOScheduler, pool: asyncpg.Pool) -> None:
    """Add all quant jobs to an existing APScheduler instance."""

    scheduler.add_job(
        _job_bhav_copy_fetch,
        trigger="cron",
        hour=16,
        minute=0,
        timezone=IST,
        id="quant_bhav_copy_fetch",
        replace_existing=True,
        misfire_grace_time=1800,
        kwargs={"pool": pool},
    )

    scheduler.add_job(
        _job_factor_compute,
        trigger="cron",
        hour=17,
        minute=0,
        timezone=IST,
        id="quant_factor_compute",
        replace_existing=True,
        misfire_grace_time=1800,
        kwargs={"pool": pool},
    )

    scheduler.add_job(
        _job_signal_returns_fill,
        trigger="cron",
        hour=17,
        minute=30,
        timezone=IST,
        id="quant_signal_returns_fill",
        replace_existing=True,
        misfire_grace_time=1800,
        kwargs={"pool": pool},
    )

    scheduler.add_job(
        _job_portfolio_rebalance,
        trigger="cron",
        day_of_week="mon",
        hour=9,
        minute=0,
        timezone=IST,
        id="quant_portfolio_rebalance",
        replace_existing=True,
        misfire_grace_time=3600,
        kwargs={"pool": pool},
    )

    scheduler.add_job(
        _job_cin_bridge_refresh,
        trigger="cron",
        day=1,
        hour=2,
        minute=0,
        timezone=IST,
        id="quant_cin_bridge_refresh",
        replace_existing=True,
        misfire_grace_time=7200,
        kwargs={"pool": pool},
    )

    logger.info("Quant scheduler jobs registered (5 jobs)")


# ─── Job implementations ──────────────────────────────────────────────────────

async def _job_bhav_copy_fetch(pool: asyncpg.Pool) -> None:
    """Download today's BSE Bhav Copy and ingest into price_daily."""
    try:
        from quant.market_data import ingest_bhav_records
        from quant.scrapers.bhav_copy_fetcher import fetch_bhav_copy

        today = date.today()
        records = await fetch_bhav_copy(today)
        if records:
            n = await ingest_bhav_records(pool, records)
            logger.info("[quant] Bhav Copy ingested: %d records for %s", n, today)
        else:
            logger.info("[quant] Bhav Copy: no data for %s (holiday?)", today)
    except Exception as exc:
        logger.error("[quant] bhav_copy_fetch failed: %s", exc)


async def _job_factor_compute(pool: asyncpg.Pool) -> None:
    """Compute all alpha factors for today."""
    try:
        from quant.factor_engine import compute_all_factors

        n = await compute_all_factors(pool)
        logger.info("[quant] Factor computation done: %d CINs", n)
    except Exception as exc:
        logger.error("[quant] factor_compute failed: %s", exc)


async def _job_signal_returns_fill(pool: asyncpg.Pool) -> None:
    """Fill forward prices and returns for elapsed signal windows."""
    try:
        from quant.backtester import fill_forward_returns

        n = await fill_forward_returns(pool)
        logger.info("[quant] Signal returns filled: %d records", n)
    except Exception as exc:
        logger.error("[quant] signal_returns_fill failed: %s", exc)


async def _job_portfolio_rebalance(pool: asyncpg.Pool) -> None:
    """Weekly portfolio rebalance (paper mode unless FEATURE_QUANT_LIVE=true)."""
    try:
        from features import feature
        from quant.execution_interface import ExecutionInterface
        from quant.portfolio_engine import build_portfolio, close_stale_positions, persist_portfolio
        from quant.risk_manager import (
            apply_risk_actions,
            check_critical_event_on_longs,
            check_open_positions,
            check_portfolio_drawdown,
        )

        today = date.today()

        # Risk checks first
        stop_actions   = await check_open_positions(pool)
        crit_actions   = await check_critical_event_on_longs(pool)
        drawdown_action = await check_portfolio_drawdown(pool)

        all_actions = stop_actions + crit_actions
        if drawdown_action:
            all_actions.append(drawdown_action)
            logger.warning("[quant] Drawdown circuit-breaker triggered — halting new entries")
            await apply_risk_actions(pool, [drawdown_action], today)
            return

        await apply_risk_actions(pool, all_actions, today)

        # Build and persist new portfolio
        portfolio = await build_portfolio(pool)
        await close_stale_positions(pool, portfolio, today)
        await persist_portfolio(pool, portfolio, today)

        executor = ExecutionInterface(pool)
        await executor.execute_portfolio(portfolio, today)

        logger.info(
            "[quant] Rebalance complete: %d long, %d short, run_id=%s",
            len(portfolio.long_positions), len(portfolio.short_positions), portfolio.run_id,
        )
    except Exception as exc:
        logger.error("[quant] portfolio_rebalance failed: %s", exc)


async def _job_cin_bridge_refresh(pool: asyncpg.Pool) -> None:
    """Monthly refresh of CIN ↔ ticker bridge from BSE/NSE master files."""
    try:
        from quant.cin_bridge import deactivate_stale_mappings, upsert_ticker_mapping
        from quant.scrapers.bse_bridge_scraper import fetch_bse_listings
        from quant.scrapers.nse_bridge_scraper import fetch_nse_listings

        bse_listings = await fetch_bse_listings()
        nse_listings = await fetch_nse_listings()
        isin_to_nse = {l.isin: l.nse_symbol for l in nse_listings if l.isin}

        updated = 0
        async with pool.acquire() as conn:
            for listing in bse_listings:
                if not listing.cin:
                    continue
                exists = await conn.fetchval(
                    "SELECT 1 FROM master_entities WHERE cin = $1", listing.cin
                )
                if not exists:
                    continue
                await upsert_ticker_mapping(
                    conn, listing.cin, "BSE", listing.bse_code,
                    listing.isin, listing.bse_code, listing.company_name, listing.sector,
                    source="BSE_MASTER",
                )
                nse_sym = isin_to_nse.get(listing.isin)
                if nse_sym:
                    await upsert_ticker_mapping(
                        conn, listing.cin, "NSE", nse_sym,
                        listing.isin, None, listing.company_name, listing.sector,
                        source="NSE_EQUITY_L",
                    )
                updated += 1

            deactivated = await deactivate_stale_mappings(conn, days_stale=45)

        logger.info("[quant] Bridge refresh: %d updated, %d deactivated", updated, deactivated)
    except Exception as exc:
        logger.error("[quant] cin_bridge_refresh failed: %s", exc)
