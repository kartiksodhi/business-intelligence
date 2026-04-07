"""
Quant module REST endpoints.

All routes under /quant/ prefix. Follows the same pattern as /op/ endpoints.
Gated behind FEATURE_QUANT feature flag.

Endpoints:
  GET  /quant/universe           — all CINs in ticker_bridge
  GET  /quant/factors/{cin}      — latest factor scores for a CIN
  GET  /quant/portfolio          — current open positions
  GET  /quant/backtest/results   — backtest run summaries
  POST /quant/backtest/run       — trigger a backtest (async)
  POST /quant/factors/compute    — trigger factor computation for today
  POST /quant/portfolio/rebalance — manual portfolio rebalance (paper mode)
"""

from __future__ import annotations

import logging
import traceback
from datetime import date
from typing import Annotated, Optional

import asyncpg
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from api.dependencies import get_db
from features import feature

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/quant", tags=["quant"])


def _require_quant() -> None:
    if not feature("QUANT_MODULE"):
        raise HTTPException(status_code=503, detail="Quant module not enabled. Set FEATURE_QUANT=true.")


def _db_error(e: Exception) -> HTTPException:
    logger.error("Quant DB error:\n%s", traceback.format_exc())
    return HTTPException(status_code=500, detail="Internal server error.")


# ─── Response models ──────────────────────────────────────────────────────────

class TickerItem(BaseModel):
    cin: str
    exchange: str
    ticker_symbol: str
    isin: Optional[str]
    company_name_listed: Optional[str]
    sector_listed: Optional[str]
    market_cap_cr: Optional[float]
    is_active: bool


class FactorScoreItem(BaseModel):
    cin: str
    score_date: date
    health_score_raw: Optional[int]
    health_score_30d_delta: Optional[float]
    health_score_90d_delta: Optional[float]
    legal_velocity_score: Optional[float]
    director_instability_score: Optional[float]
    filing_decay_score: Optional[float]
    workforce_momentum: Optional[float]
    government_revenue_signal: Optional[float]
    leverage_creep_signal: Optional[float]
    distress_composite: Optional[float]
    promoter_stress_signal: Optional[float]
    sector_stress_percentile: Optional[float]
    composite_alpha_score: Optional[float]
    signal_fired: list[str]
    computed_at: str


class PortfolioPositionItem(BaseModel):
    id: int
    run_id: str
    cin: Optional[str]
    ticker_symbol: str
    exchange: str
    direction: str
    entry_date: date
    exit_date: Optional[date]
    entry_price: Optional[float]
    weight_pct: Optional[float]
    entry_composite: Optional[float]
    entry_signals: list[str]
    pnl_pct: Optional[float]
    is_open: bool
    exit_reason: Optional[str]


class BacktestRunItem(BaseModel):
    id: int
    run_name: str
    universe_size: Optional[int]
    start_date: date
    end_date: date
    total_signals: Optional[int]
    long_count: Optional[int]
    short_count: Optional[int]
    annualized_return: Optional[float]
    sharpe_ratio: Optional[float]
    max_drawdown: Optional[float]
    win_rate: Optional[float]
    ic_30d: Optional[float]
    created_at: str


class BacktestRunRequest(BaseModel):
    run_name: str
    start_date: date
    end_date: date


class ComputeFactorsResponse(BaseModel):
    cins_processed: int
    score_date: date


class RebalanceResponse(BaseModel):
    run_id: str
    long_count: int
    short_count: int
    gross_exposure: float
    net_exposure: float
    mode: str


# ─── Endpoints ────────────────────────────────────────────────────────────────

@router.get("/universe", response_model=list[TickerItem])
async def get_universe(
    db: Annotated[asyncpg.Connection, Depends(get_db)],
    exchange: Optional[str] = Query(None, description="Filter by exchange: BSE or NSE"),
    active_only: bool = Query(True),
):
    """List all companies in the ticker bridge (the investable universe)."""
    _require_quant()
    try:
        where_clauses = []
        params = []
        if exchange:
            params.append(exchange.upper())
            where_clauses.append(f"exchange = ${len(params)}")
        if active_only:
            where_clauses.append("is_active = TRUE")

        where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
        rows = await db.fetch(
            f"""
            SELECT cin, exchange, ticker_symbol, isin, company_name_listed,
                   sector_listed, market_cap_cr, is_active
            FROM ticker_bridge
            {where_sql}
            ORDER BY cin
            """,
            *params,
        )
        return [dict(r) for r in rows]
    except Exception as e:
        raise _db_error(e)


@router.get("/factors/{cin}", response_model=list[FactorScoreItem])
async def get_factors(
    cin: str,
    db: Annotated[asyncpg.Connection, Depends(get_db)],
    days: int = Query(30, description="Number of recent days to return"),
):
    """Return factor score history for a specific CIN."""
    _require_quant()
    try:
        rows = await db.fetch(
            """
            SELECT cin, score_date, health_score_raw, health_score_30d_delta,
                   health_score_90d_delta, legal_velocity_score, director_instability_score,
                   filing_decay_score, workforce_momentum, government_revenue_signal,
                   leverage_creep_signal, distress_composite, promoter_stress_signal,
                   sector_stress_percentile, composite_alpha_score, signal_fired,
                   computed_at::text AS computed_at
            FROM factor_scores
            WHERE cin = $1
              AND score_date >= CURRENT_DATE - ($2 || ' days')::INTERVAL
            ORDER BY score_date DESC
            """,
            cin, str(days),
        )
        return [
            {**dict(r), "signal_fired": list(r["signal_fired"] or [])}
            for r in rows
        ]
    except Exception as e:
        raise _db_error(e)


@router.get("/portfolio", response_model=list[PortfolioPositionItem])
async def get_portfolio(
    db: Annotated[asyncpg.Connection, Depends(get_db)],
    open_only: bool = Query(True),
    limit: int = Query(100),
):
    """Return current open positions (or recent history)."""
    _require_quant()
    try:
        where = "WHERE is_open = TRUE" if open_only else ""
        rows = await db.fetch(
            f"""
            SELECT id, run_id, cin, ticker_symbol, exchange, direction,
                   entry_date, exit_date, entry_price, weight_pct,
                   entry_composite, entry_signals, pnl_pct, is_open, exit_reason
            FROM portfolio_positions
            {where}
            ORDER BY entry_date DESC
            LIMIT $1
            """,
            limit,
        )
        return [
            {**dict(r), "entry_signals": list(r["entry_signals"] or [])}
            for r in rows
        ]
    except Exception as e:
        raise _db_error(e)


@router.get("/backtest/results", response_model=list[BacktestRunItem])
async def get_backtest_results(
    db: Annotated[asyncpg.Connection, Depends(get_db)],
    limit: int = Query(20),
):
    """Return recent backtest run summaries."""
    _require_quant()
    try:
        rows = await db.fetch(
            """
            SELECT id, run_name, universe_size, start_date, end_date,
                   total_signals, long_count, short_count, annualized_return,
                   sharpe_ratio, max_drawdown, win_rate, ic_30d,
                   created_at::text AS created_at
            FROM backtest_runs
            ORDER BY created_at DESC
            LIMIT $1
            """,
            limit,
        )
        return [dict(r) for r in rows]
    except Exception as e:
        raise _db_error(e)


@router.post("/backtest/run", response_model=BacktestRunItem)
async def trigger_backtest(
    req: BacktestRunRequest,
    db: Annotated[asyncpg.Connection, Depends(get_db)],
):
    """
    Trigger a backtest run for the given date range.
    Seeds signal_returns for the period, then computes IC metrics.
    """
    _require_quant()
    try:
        import asyncpg as _asyncpg
        import os
        pool = await _asyncpg.create_pool(os.environ["DATABASE_URL"], min_size=1, max_size=3)

        from quant.backtester import compute_ic, save_backtest_run, seed_signal_returns
        await seed_signal_returns(pool, req.start_date, req.end_date)
        results = await compute_ic(pool)

        run_id = await save_backtest_run(
            pool, req.run_name, results, req.start_date, req.end_date,
            strategy_config={"start": str(req.start_date), "end": str(req.end_date)},
        )
        await pool.close()

        row = await db.fetchrow(
            "SELECT *, created_at::text AS created_at FROM backtest_runs WHERE id = $1",
            run_id,
        )
        if not row:
            raise HTTPException(status_code=404, detail="Backtest run not found after save")
        return dict(row)
    except HTTPException:
        raise
    except Exception as e:
        raise _db_error(e)


@router.post("/factors/compute", response_model=ComputeFactorsResponse)
async def trigger_factor_compute(
    db: Annotated[asyncpg.Connection, Depends(get_db)],
    score_date: Optional[date] = Query(None),
):
    """Trigger factor computation for a specific date (default: today)."""
    _require_quant()
    try:
        import asyncpg as _asyncpg
        import os
        pool = await _asyncpg.create_pool(os.environ["DATABASE_URL"], min_size=1, max_size=5)

        from quant.factor_engine import compute_all_factors
        n = await compute_all_factors(pool, score_date)
        await pool.close()

        return {"cins_processed": n, "score_date": score_date or date.today()}
    except Exception as e:
        raise _db_error(e)


@router.post("/portfolio/rebalance", response_model=RebalanceResponse)
async def trigger_rebalance(
    db: Annotated[asyncpg.Connection, Depends(get_db)],
):
    """
    Trigger a portfolio rebalance. Always runs in paper mode unless
    FEATURE_QUANT_LIVE=true is set.
    """
    _require_quant()
    try:
        import asyncpg as _asyncpg
        import os
        pool = await _asyncpg.create_pool(os.environ["DATABASE_URL"], min_size=1, max_size=5)

        from quant.execution_interface import ExecutionInterface
        from quant.portfolio_engine import build_portfolio, close_stale_positions, persist_portfolio
        from quant.risk_manager import check_critical_event_on_longs, check_open_positions

        today = date.today()

        # Risk checks first
        stop_actions = await check_open_positions(pool)
        crit_actions = await check_critical_event_on_longs(pool)

        executor = ExecutionInterface(pool)
        await executor.process_risk_actions(stop_actions + crit_actions, today)

        # Build new portfolio
        portfolio = build_portfolio(pool)
        await close_stale_positions(pool, portfolio, today)
        await persist_portfolio(pool, portfolio, today)

        await pool.close()

        return RebalanceResponse(
            run_id=portfolio.run_id,
            long_count=len(portfolio.long_positions),
            short_count=len(portfolio.short_positions),
            gross_exposure=portfolio.gross_exposure,
            net_exposure=portfolio.net_exposure,
            mode="PAPER" if not feature("QUANT_LIVE") else "LIVE",
        )
    except Exception as e:
        raise _db_error(e)
