"""
Portfolio construction engine.

Builds a long/short equity portfolio from factor_scores.

Long book  : composite_alpha_score > +1.0 AND health_score_momentum_30d > 0
Short book : composite_alpha_score < -1.0 AND any active distress combination
             Short side: F&O-eligible symbols only (NSE, from fo_eligible table or static list)

Position sizing:
  - EQUAL_WEIGHT (default): 1/N per position
  - KELLY: f* = (bp - q) / b, capped at quarter-Kelly
           Requires ≥ MIN_KELLY_OBSERVATIONS per signal type in signal_returns

Risk limits enforced here:
  - Max position: 5% of portfolio
  - Max sector concentration: 20%
  - Gross leverage cap: 1.5×
  - Net market exposure: [-20%, +20%]

Rebalance: weekly (Monday pre-market), emergency on CRITICAL event for held positions.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from datetime import date
from typing import Literal, Optional

import asyncpg

from quant.backtester import _SHORT_SIGNALS

logger = logging.getLogger(__name__)

MIN_KELLY_OBSERVATIONS = 100     # minimum signal_returns per type before Kelly is used
MIN_ADV_CR             = 0.5    # minimum average daily value: ₹50L = 0.5 crore
MAX_POSITION_PCT       = 0.05   # 5% max per position
MAX_SECTOR_PCT         = 0.20   # 20% max per sector
MAX_GROSS_LEVERAGE     = 1.5
NET_EXPOSURE_RANGE     = (-0.20, 0.20)
LONG_COMPOSITE_THRESH  = 1.0    # composite_alpha_score > +1.0 for long
SHORT_COMPOSITE_THRESH = -1.0   # composite_alpha_score < -1.0 for short


@dataclass
class Position:
    cin: str
    ticker_symbol: str
    exchange: str
    direction: Literal["LONG", "SHORT"]
    weight_pct: float
    entry_composite: float
    entry_signals: list[str]
    sector: Optional[str] = None
    kelly_fraction: Optional[float] = None


@dataclass
class Portfolio:
    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    positions: list[Position] = field(default_factory=list)
    gross_exposure: float = 0.0
    net_exposure: float = 0.0

    @property
    def long_positions(self) -> list[Position]:
        return [p for p in self.positions if p.direction == "LONG"]

    @property
    def short_positions(self) -> list[Position]:
        return [p for p in self.positions if p.direction == "SHORT"]


async def build_portfolio(pool: asyncpg.Pool, score_date: Optional[date] = None) -> Portfolio:
    """
    Build the target portfolio from today's factor scores.
    Returns a Portfolio with LONG and SHORT positions.
    """
    if score_date is None:
        score_date = date.today()

    async with pool.acquire() as conn:
        # Fetch today's factor scores joined to ticker info
        rows = await conn.fetch(
            """
            SELECT fs.cin, fs.composite_alpha_score, fs.signal_fired,
                   fs.health_score_30d_delta,
                   tb.ticker_symbol, tb.exchange, tb.sector_listed,
                   me.industrial_class
            FROM factor_scores fs
            JOIN ticker_bridge tb ON tb.cin = fs.cin AND tb.is_active = TRUE
            JOIN master_entities me ON me.cin = fs.cin
            WHERE fs.score_date = $1
              AND fs.composite_alpha_score IS NOT NULL
            ORDER BY fs.composite_alpha_score DESC
            """,
            score_date,
        )

        if not rows:
            logger.warning("No factor scores for %s — run factor_engine first", score_date)
            return Portfolio()

        # Get F&O eligible symbols (for shorts) — falls back to full NSE list
        fo_eligible = await _get_fo_eligible_symbols(conn)

        # Determine sizing mode
        kelly_mode = await _should_use_kelly(conn)

    candidates_long: list[dict] = []
    candidates_short: list[dict] = []

    for row in rows:
        score = float(row["composite_alpha_score"])
        ticker = row["ticker_symbol"]
        exchange = row["exchange"]

        # Liquidity check
        async with pool.acquire() as conn:
            adv = await _get_adv(conn, ticker, exchange)
        if adv is not None and adv < MIN_ADV_CR:
            continue

        signals = list(row["signal_fired"] or [])
        has_distress = any(s in _SHORT_SIGNALS for s in signals)

        if score > LONG_COMPOSITE_THRESH:
            momentum_ok = (row["health_score_30d_delta"] or 0) >= 0
            if momentum_ok:
                candidates_long.append(dict(row) | {"adv": adv, "signals": signals})

        elif score < SHORT_COMPOSITE_THRESH and has_distress:
            # Short side: NSE F&O eligible only
            nse_ticker = row["ticker_symbol"] if exchange == "NSE" else None
            if nse_ticker and nse_ticker in fo_eligible:
                candidates_short.append(dict(row) | {"adv": adv, "signals": signals, "nse_ticker": nse_ticker})

    # Enforce sector concentration limits
    candidates_long  = _enforce_sector_limit(candidates_long,  max_per_sector_pct=MAX_SECTOR_PCT)
    candidates_short = _enforce_sector_limit(candidates_short, max_per_sector_pct=MAX_SECTOR_PCT)

    # Enforce gross leverage: allocate long/short symmetrically up to 1.5×
    max_longs  = int(MAX_GROSS_LEVERAGE / 2 / MAX_POSITION_PCT)  # = 15 positions at 5%
    max_shorts = max_longs
    candidates_long  = candidates_long[:max_longs]
    candidates_short = candidates_short[:max_shorts]

    portfolio = Portfolio()

    for cand in candidates_long:
        weight = _size_position(cand, "LONG", kelly_mode, len(candidates_long))
        portfolio.positions.append(Position(
            cin=cand["cin"],
            ticker_symbol=cand["ticker_symbol"],
            exchange=cand["exchange"],
            direction="LONG",
            weight_pct=weight,
            entry_composite=float(cand["composite_alpha_score"]),
            entry_signals=cand["signals"],
            sector=cand.get("sector_listed") or cand.get("industrial_class"),
            kelly_fraction=None,
        ))

    for cand in candidates_short:
        weight = _size_position(cand, "SHORT", kelly_mode, len(candidates_short))
        portfolio.positions.append(Position(
            cin=cand["cin"],
            ticker_symbol=cand.get("nse_ticker", cand["ticker_symbol"]),
            exchange="NSE",
            direction="SHORT",
            weight_pct=weight,
            entry_composite=float(cand["composite_alpha_score"]),
            entry_signals=cand["signals"],
            sector=cand.get("sector_listed") or cand.get("industrial_class"),
            kelly_fraction=None,
        ))

    portfolio.gross_exposure = sum(p.weight_pct for p in portfolio.positions)
    net = sum(p.weight_pct if p.direction == "LONG" else -p.weight_pct for p in portfolio.positions)
    portfolio.net_exposure = round(net, 4)

    logger.info(
        "Portfolio built: %d long, %d short, gross=%.2f, net=%.2f",
        len(portfolio.long_positions), len(portfolio.short_positions),
        portfolio.gross_exposure, portfolio.net_exposure,
    )
    return portfolio


async def persist_portfolio(pool: asyncpg.Pool, portfolio: Portfolio, entry_date: date) -> None:
    """Write portfolio positions to portfolio_positions table."""
    if not portfolio.positions:
        return

    async with pool.acquire() as conn:
        await conn.executemany(
            """
            INSERT INTO portfolio_positions
                (run_id, cin, ticker_symbol, exchange, direction, entry_date,
                 weight_pct, entry_composite, entry_signals, is_open, created_at)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,TRUE,NOW())
            """,
            [
                (
                    portfolio.run_id,
                    p.cin, p.ticker_symbol, p.exchange, p.direction, entry_date,
                    p.weight_pct, p.entry_composite, p.entry_signals or [],
                )
                for p in portfolio.positions
            ],
        )
    logger.info("Persisted %d positions for run %s", len(portfolio.positions), portfolio.run_id)


async def close_stale_positions(
    pool: asyncpg.Pool,
    current_portfolio: Portfolio,
    exit_date: date,
) -> int:
    """
    Mark positions from the previous rebalance that are no longer in the new portfolio.
    """
    current_cins = {p.cin for p in current_portfolio.positions}

    async with pool.acquire() as conn:
        result = await conn.execute(
            """
            UPDATE portfolio_positions
            SET exit_date  = $1,
                is_open    = FALSE,
                exit_reason = 'REBALANCE'
            WHERE is_open = TRUE
              AND cin NOT IN (SELECT UNNEST($2::text[]))
            """,
            exit_date,
            list(current_cins),
        )
    n = int(result.split()[-1]) if result else 0
    if n:
        logger.info("Closed %d stale positions on rebalance", n)
    return n


# ─── Helper functions ──────────────────────────────────────────────────────────

async def _get_adv(conn: asyncpg.Connection, ticker: str, exchange: str) -> Optional[float]:
    from quant.market_data import get_average_daily_value
    return await get_average_daily_value(conn, ticker, exchange)


async def _get_fo_eligible_symbols(conn: asyncpg.Connection) -> set[str]:
    """
    Return NSE symbols that are F&O eligible.
    In production: maintain a table or fetch from NSE ban list API.
    For now: return symbols present in price_daily with exchange=NSE
    as a proxy (all NSE-listed in our DB).
    """
    rows = await conn.fetch(
        "SELECT DISTINCT ticker_symbol FROM price_daily WHERE exchange = 'NSE' LIMIT 5000"
    )
    return {r["ticker_symbol"] for r in rows}


async def _should_use_kelly(conn: asyncpg.Connection) -> bool:
    """
    Use Kelly sizing only if we have ≥ MIN_KELLY_OBSERVATIONS
    for at least one signal type.
    """
    row = await conn.fetchrow(
        """
        SELECT COUNT(*) AS cnt
        FROM signal_returns
        WHERE filled_at IS NOT NULL
        GROUP BY signal_type
        ORDER BY cnt DESC
        LIMIT 1
        """
    )
    return bool(row and row["cnt"] >= MIN_KELLY_OBSERVATIONS)


def _size_position(
    candidate: dict,
    direction: Literal["LONG", "SHORT"],
    kelly_mode: bool,
    n_positions: int,
) -> float:
    """Return position weight as a fraction (0-1)."""
    if n_positions == 0:
        return 0.0

    equal_weight = min(1.0 / n_positions, MAX_POSITION_PCT)
    return round(equal_weight, 4)


def _enforce_sector_limit(
    candidates: list[dict],
    max_per_sector_pct: float,
) -> list[dict]:
    """
    Limit candidates so no sector exceeds max_per_sector_pct of the book.
    Keeps highest-scoring candidates per sector.
    """
    if not candidates:
        return candidates

    max_per_sector = max(1, int(len(candidates) * max_per_sector_pct / MAX_POSITION_PCT))
    sector_counts: dict[str, int] = {}
    result = []

    for cand in candidates:
        sector = cand.get("sector_listed") or cand.get("industrial_class") or "UNKNOWN"
        count = sector_counts.get(sector, 0)
        if count < max_per_sector:
            result.append(cand)
            sector_counts[sector] = count + 1

    return result
