"""
Signal backtester.

Walks historical events, links them to price data via ticker_bridge,
and measures forward returns at 30/60/90 day horizons.

Two modes:
  1. seed_signal_returns()  — one-time historical backfill
  2. fill_forward_returns() — daily job that fills in returns for
                              elapsed windows (called by quant_scheduler.py)

Key metric: Information Coefficient (IC) = Spearman rank correlation
between composite_alpha_score at signal date and forward return.
IC > 0.05 indicates a detectable predictive signal.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Optional

import asyncpg
import numpy as np
from scipy import stats

from quant.market_data import get_close_price, get_forward_price

logger = logging.getLogger(__name__)

# Signals that imply a SHORT thesis (distress → sell)
_SHORT_SIGNALS = frozenset({
    "PRE_INSOLVENCY_CLASSIC",
    "NPA_SEIZURE_IMMINENT",
    "GOVERNANCE_COLLAPSE",
    "OPERATIONAL_SHUTDOWN",
    "PROMOTER_EXIT_BANK_SEIZURE",
    "SARFAESI_DEMAND",
    "SARFAESI_POSSESSION",
    "SARFAESI_AUCTION",
    "NCLT_ADMISSION",
    "NCLT_LIQUIDATION",
    "GST_CANCELLED",
    "RBI_WILFUL_DEFAULTER",
    "SEBI_ENFORCEMENT_ORDER",
    "EPFO_CONTRIBUTION_DROP",
    "FILING_DECAY",
})

# Signals that imply a LONG thesis (growth/recovery → buy)
_LONG_SIGNALS = frozenset({
    "FUNDED_GROWTH",
    "CAPITAL_INCREASE",
    "GEM_ORDER_WON",
    "EPFO_HIRING_SURGE",
    "MCA_CHARGE_SATISFIED",
})

# Nifty 50 BSE code for benchmark returns
_NIFTY_SYMBOL = "999901"


@dataclass
class BacktestResult:
    signal_type: str
    n_signals: int
    n_long: int
    n_short: int
    ic_30d: Optional[float]
    ic_60d: Optional[float]
    ic_90d: Optional[float]
    avg_alpha_30d: Optional[float]
    avg_alpha_60d: Optional[float]
    avg_alpha_90d: Optional[float]
    hit_rate_30d: Optional[float]
    sharpe_30d: Optional[float]


async def seed_signal_returns(
    pool: asyncpg.Pool,
    start_date: date,
    end_date: date,
) -> int:
    """
    Seed signal_returns with historical signal events.
    Only includes events for CINs present in ticker_bridge.
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT e.id, e.cin, e.event_type, e.detected_at::date AS signal_date,
                   tb.ticker_symbol, tb.exchange
            FROM events e
            JOIN ticker_bridge tb ON tb.cin = e.cin AND tb.exchange = 'BSE' AND tb.is_active = TRUE
            WHERE e.detected_at::date BETWEEN $1 AND $2
              AND e.event_type = ANY($3::text[])
              AND NOT EXISTS (
                SELECT 1 FROM signal_returns sr
                WHERE sr.event_id = e.id
              )
            ORDER BY e.detected_at
            """,
            start_date,
            end_date,
            list(_SHORT_SIGNALS | _LONG_SIGNALS),
        )

    if not rows:
        logger.info("No new historical signals to seed")
        return 0

    inserted = 0
    async with pool.acquire() as conn:
        for row in rows:
            event_type = row["event_type"]
            direction = "SHORT" if event_type in _SHORT_SIGNALS else "LONG"

            signal_date: date = row["signal_date"]
            price_at_signal = await get_close_price(conn, row["ticker_symbol"], signal_date, row["exchange"])

            await conn.execute(
                """
                INSERT INTO signal_returns
                    (event_id, cin, ticker_symbol, signal_type, signal_date,
                     direction, price_at_signal, created_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
                ON CONFLICT DO NOTHING
                """,
                row["id"], row["cin"], row["ticker_symbol"],
                event_type, signal_date, direction, price_at_signal,
            )
            inserted += 1

    logger.info("Seeded %d signal_returns records", inserted)
    return inserted


async def fill_forward_returns(pool: asyncpg.Pool) -> int:
    """
    Fill in forward prices and returns for signal_returns rows where
    the forward period has elapsed but filled_at is NULL.
    Called daily at 5:30 PM IST.
    """
    today = date.today()
    filled = 0

    async with pool.acquire() as conn:
        # Fetch all unfilled rows where at least 30 days have passed
        rows = await conn.fetch(
            """
            SELECT id, cin, ticker_symbol, signal_date, direction, price_at_signal,
                   price_30d, price_60d, price_90d
            FROM signal_returns
            WHERE filled_at IS NULL
              AND signal_date <= $1
            ORDER BY signal_date
            """,
            today - timedelta(days=30),
        )

        # Nifty benchmark price on each signal date (for alpha calculation)
        nifty_cache: dict[tuple[date, int], Optional[float]] = {}

        async def nifty_price(d: date, days_fwd: int) -> Optional[float]:
            key = (d, days_fwd)
            if key not in nifty_cache:
                if days_fwd == 0:
                    nifty_cache[key] = await get_close_price(conn, _NIFTY_SYMBOL, d)
                else:
                    nifty_cache[key] = await get_forward_price(conn, _NIFTY_SYMBOL, d, days_fwd)
            return nifty_cache[key]

        for row in rows:
            signal_date: date = row["signal_date"]
            days_elapsed = (today - signal_date).days

            p0     = row["price_at_signal"]
            ticker = row["ticker_symbol"]

            updates: dict = {}

            if days_elapsed >= 30 and row["price_30d"] is None:
                updates["price_30d"] = await get_forward_price(conn, ticker, signal_date, 30)

            if days_elapsed >= 60 and row["price_60d"] is None:
                updates["price_60d"] = await get_forward_price(conn, ticker, signal_date, 60)

            if days_elapsed >= 90 and row["price_90d"] is None:
                updates["price_90d"] = await get_forward_price(conn, ticker, signal_date, 90)

            if not updates:
                continue

            # Calculate returns
            def _ret(p_fwd: Optional[float]) -> Optional[float]:
                if p0 and p0 > 0 and p_fwd:
                    raw = (p_fwd - p0) / p0
                    # Invert for SHORT positions
                    return -raw if row["direction"] == "SHORT" else raw
                return None

            p0_nifty = await nifty_price(signal_date, 0)

            def _nifty_ret(days: int) -> Optional[float]:
                p_nifty = nifty_cache.get((signal_date, days))
                if p0_nifty and p0_nifty > 0 and p_nifty:
                    return (p_nifty - p0_nifty) / p0_nifty
                return None

            ret_30 = _ret(updates.get("price_30d"))
            ret_60 = _ret(updates.get("price_60d"))
            ret_90 = _ret(updates.get("price_90d"))

            n30 = _nifty_ret(30) if "price_30d" in updates else None
            n60 = _nifty_ret(60) if "price_60d" in updates else None
            n90 = _nifty_ret(90) if "price_90d" in updates else None

            all_filled = days_elapsed >= 90 and "price_90d" in updates

            await conn.execute(
                """
                UPDATE signal_returns
                SET price_30d        = COALESCE($2, price_30d),
                    price_60d        = COALESCE($3, price_60d),
                    price_90d        = COALESCE($4, price_90d),
                    return_30d       = COALESCE($5, return_30d),
                    return_60d       = COALESCE($6, return_60d),
                    return_90d       = COALESCE($7, return_90d),
                    nifty_return_30d = COALESCE($8, nifty_return_30d),
                    nifty_return_60d = COALESCE($9, nifty_return_60d),
                    nifty_return_90d = COALESCE($10, nifty_return_90d),
                    alpha_30d        = COALESCE($11, alpha_30d),
                    alpha_60d        = COALESCE($12, alpha_60d),
                    alpha_90d        = COALESCE($13, alpha_90d),
                    filled_at        = CASE WHEN $14 THEN NOW() ELSE filled_at END
                WHERE id = $1
                """,
                row["id"],
                updates.get("price_30d"), updates.get("price_60d"), updates.get("price_90d"),
                ret_30, ret_60, ret_90,
                n30, n60, n90,
                (ret_30 - n30) if ret_30 is not None and n30 is not None else None,
                (ret_60 - n60) if ret_60 is not None and n60 is not None else None,
                (ret_90 - n90) if ret_90 is not None and n90 is not None else None,
                all_filled,
            )
            filled += 1

    logger.info("Filled forward returns for %d signal records", filled)
    return filled


async def compute_ic(
    pool: asyncpg.Pool,
    signal_type: Optional[str] = None,
    min_observations: int = 20,
) -> list[BacktestResult]:
    """
    Compute Information Coefficient and alpha metrics per signal type.
    IC = Spearman rank correlation of composite_alpha_score vs forward return.
    """
    async with pool.acquire() as conn:
        where = "AND sr.signal_type = $1" if signal_type else ""
        params = [signal_type] if signal_type else []

        rows = await conn.fetch(
            f"""
            SELECT sr.signal_type, sr.direction,
                   fs.composite_alpha_score,
                   sr.alpha_30d, sr.alpha_60d, sr.alpha_90d,
                   sr.return_30d
            FROM signal_returns sr
            LEFT JOIN factor_scores fs
                ON fs.cin = sr.cin AND fs.score_date = sr.signal_date
            WHERE sr.filled_at IS NOT NULL
              AND sr.return_30d IS NOT NULL
              {where}
            """,
            *params,
        )

    if not rows:
        return []

    import pandas as pd
    df = pd.DataFrame([dict(r) for r in rows])

    results: list[BacktestResult] = []
    for stype, grp in df.groupby("signal_type"):
        if len(grp) < min_observations:
            continue

        def _ic(score_col: str, return_col: str) -> Optional[float]:
            valid = grp[[score_col, return_col]].dropna()
            if len(valid) < 10:
                return None
            corr, _ = stats.spearmanr(valid[score_col], valid[return_col])
            return round(float(corr), 4)

        def _avg(col: str) -> Optional[float]:
            v = grp[col].dropna()
            return round(float(v.mean()), 4) if len(v) > 0 else None

        def _hit_rate(col: str) -> Optional[float]:
            v = grp[col].dropna()
            return round(float((v > 0).mean()), 4) if len(v) > 0 else None

        def _sharpe(col: str) -> Optional[float]:
            v = grp[col].dropna()
            if len(v) < 5 or v.std() == 0:
                return None
            return round(float(v.mean() / v.std() * (252 ** 0.5)), 4)

        ic_30 = _ic("composite_alpha_score", "alpha_30d") if "composite_alpha_score" in grp.columns else None

        results.append(BacktestResult(
            signal_type=str(stype),
            n_signals=len(grp),
            n_long=int((grp["direction"] == "LONG").sum()),
            n_short=int((grp["direction"] == "SHORT").sum()),
            ic_30d=ic_30,
            ic_60d=None,
            ic_90d=None,
            avg_alpha_30d=_avg("alpha_30d"),
            avg_alpha_60d=_avg("alpha_60d"),
            avg_alpha_90d=_avg("alpha_90d"),
            hit_rate_30d=_hit_rate("alpha_30d"),
            sharpe_30d=_sharpe("alpha_30d"),
        ))

    results.sort(key=lambda r: r.n_signals, reverse=True)
    return results


async def save_backtest_run(
    pool: asyncpg.Pool,
    run_name: str,
    results: list[BacktestResult],
    start_date: date,
    end_date: date,
    strategy_config: dict,
) -> int:
    """Persist aggregate backtest metrics to backtest_runs."""
    if not results:
        return 0

    import statistics

    total = sum(r.n_signals for r in results)
    long_c = sum(r.n_long for r in results)
    short_c = sum(r.n_short for r in results)

    valid_ic  = [r.ic_30d    for r in results if r.ic_30d    is not None]
    valid_ret = [r.avg_alpha_30d for r in results if r.avg_alpha_30d is not None]
    valid_hr  = [r.hit_rate_30d  for r in results if r.hit_rate_30d  is not None]
    valid_sh  = [r.sharpe_30d    for r in results if r.sharpe_30d    is not None]

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO backtest_runs
                (run_name, strategy_config, universe_size, start_date, end_date,
                 total_signals, long_count, short_count,
                 annualized_return, sharpe_ratio, win_rate, ic_30d, created_at)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,NOW())
            RETURNING id
            """,
            run_name,
            json.dumps(strategy_config),
            len(results),
            start_date, end_date,
            total, long_c, short_c,
            statistics.mean(valid_ret) * 252 if valid_ret else None,
            statistics.mean(valid_sh) if valid_sh else None,
            statistics.mean(valid_hr) if valid_hr else None,
            statistics.mean(valid_ic) if valid_ic else None,
        )
    return row["id"] if row else 0
