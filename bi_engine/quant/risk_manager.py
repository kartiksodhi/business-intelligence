"""
Risk manager.

Enforces position limits, stop-losses, and portfolio-level circuit-breakers.

Rules:
  - Stop-loss: -15% on any single position → force exit
  - Drawdown circuit-breaker: -10% portfolio drawdown → halt new entries,
    send CRITICAL Telegram alert
  - Max position: 5% (enforced in portfolio_engine, validated here)
  - Max sector: 20% of book
  - Gross leverage cap: 1.5×
  - Net market exposure: -20% to +20%
  - CRITICAL event on any held LONG → emergency exit signal

The risk manager never places orders itself. It emits RiskAction objects
that the execution_interface acts on.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date
from typing import Literal, Optional

import asyncpg

from quant.market_data import get_close_price

logger = logging.getLogger(__name__)

# Thresholds
STOP_LOSS_PCT          = -0.15   # -15% return on single position
DRAWDOWN_CIRCUIT_PCT   = -0.10   # -10% portfolio drawdown
MAX_POSITION_PCT       = 0.05
MAX_SECTOR_PCT         = 0.20
MAX_GROSS_LEVERAGE     = 1.5


@dataclass
class RiskAction:
    action: Literal["EXIT_POSITION", "HALT_ENTRIES", "RESUME_ENTRIES", "NO_ACTION"]
    cin: Optional[str] = None
    ticker_symbol: Optional[str] = None
    reason: str = ""
    severity: Literal["INFO", "WATCH", "ALERT", "CRITICAL"] = "INFO"


async def check_open_positions(pool: asyncpg.Pool) -> list[RiskAction]:
    """
    Scan all open positions for stop-loss breaches.
    Returns list of RiskActions for the execution_interface to process.
    """
    actions: list[RiskAction] = []
    today = date.today()

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, cin, ticker_symbol, exchange, direction,
                   entry_price, weight_pct, entry_date
            FROM portfolio_positions
            WHERE is_open = TRUE AND entry_price IS NOT NULL
            """
        )

        for row in rows:
            current_price = await get_close_price(
                conn, row["ticker_symbol"], today, row["exchange"]
            )
            if current_price is None or row["entry_price"] is None:
                continue

            entry = float(row["entry_price"])
            if entry <= 0:
                continue

            raw_ret = (current_price - entry) / entry

            # For SHORT positions, gain when price falls
            position_return = -raw_ret if row["direction"] == "SHORT" else raw_ret

            if position_return <= STOP_LOSS_PCT:
                actions.append(RiskAction(
                    action="EXIT_POSITION",
                    cin=row["cin"],
                    ticker_symbol=row["ticker_symbol"],
                    reason=f"Stop-loss hit: {position_return:.1%}",
                    severity="ALERT",
                ))
                logger.warning(
                    "Stop-loss triggered for %s (%s): return=%.2f%%",
                    row["ticker_symbol"], row["direction"], position_return * 100,
                )

    return actions


async def check_portfolio_drawdown(pool: asyncpg.Pool) -> Optional[RiskAction]:
    """
    Compute portfolio-level drawdown from closed positions P&L.
    If drawdown > 10%, return HALT_ENTRIES action.
    """
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT SUM(pnl_pct) AS total_pnl,
                   SUM(CASE WHEN pnl_pct < 0 THEN pnl_pct ELSE 0 END) AS total_loss
            FROM portfolio_positions
            WHERE is_open = FALSE
              AND exit_date >= CURRENT_DATE - INTERVAL '30 days'
              AND pnl_pct IS NOT NULL
            """
        )

        if not row or row["total_loss"] is None:
            return None

        drawdown = float(row["total_loss"])
        if drawdown <= DRAWDOWN_CIRCUIT_PCT:
            return RiskAction(
                action="HALT_ENTRIES",
                reason=f"Portfolio drawdown {drawdown:.1%} exceeds -10% threshold",
                severity="CRITICAL",
            )

    return None


async def check_critical_event_on_longs(pool: asyncpg.Pool) -> list[RiskAction]:
    """
    Scan for CRITICAL events fired on currently held LONG positions.
    These require immediate exit regardless of stop-loss.
    """
    actions: list[RiskAction] = []

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT pp.cin, pp.ticker_symbol
            FROM portfolio_positions pp
            WHERE pp.is_open = TRUE
              AND pp.direction = 'LONG'
              AND EXISTS (
                SELECT 1 FROM events e
                WHERE e.cin = pp.cin
                  AND e.severity = 'CRITICAL'
                  AND e.detected_at >= pp.created_at
                  AND e.detected_at >= NOW() - INTERVAL '24 hours'
              )
            """
        )

        for row in rows:
            actions.append(RiskAction(
                action="EXIT_POSITION",
                cin=row["cin"],
                ticker_symbol=row["ticker_symbol"],
                reason="CRITICAL event fired on held LONG position",
                severity="CRITICAL",
            ))
            logger.warning(
                "Emergency exit triggered for LONG %s due to CRITICAL event",
                row["ticker_symbol"],
            )

    return actions


async def validate_portfolio_limits(
    positions: list[dict],
) -> list[str]:
    """
    Pre-trade validation. Returns list of violations (empty = OK).
    Called by portfolio_engine before persisting.
    """
    violations = []

    total_weight = sum(abs(p.get("weight_pct", 0)) for p in positions)
    if total_weight > MAX_GROSS_LEVERAGE:
        violations.append(f"Gross leverage {total_weight:.2f}x exceeds cap {MAX_GROSS_LEVERAGE}x")

    for p in positions:
        if abs(p.get("weight_pct", 0)) > MAX_POSITION_PCT + 0.001:
            violations.append(
                f"Position {p.get('ticker_symbol')} weight {p.get('weight_pct'):.2%} "
                f"exceeds max {MAX_POSITION_PCT:.0%}"
            )

    sector_weights: dict[str, float] = {}
    for p in positions:
        sec = p.get("sector") or "UNKNOWN"
        sector_weights[sec] = sector_weights.get(sec, 0) + abs(p.get("weight_pct", 0))
    for sec, w in sector_weights.items():
        if w > MAX_SECTOR_PCT:
            violations.append(f"Sector {sec} weight {w:.2%} exceeds max {MAX_SECTOR_PCT:.0%}")

    return violations


async def apply_risk_actions(
    pool: asyncpg.Pool,
    actions: list[RiskAction],
    exit_date: date,
) -> None:
    """
    Apply EXIT_POSITION actions: mark positions as closed in DB.
    HALT_ENTRIES actions: send Telegram alert (handled by execution_interface).
    """
    if not actions:
        return

    exit_cins = [a.cin for a in actions if a.action == "EXIT_POSITION" and a.cin]

    if exit_cins:
        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE portfolio_positions
                SET is_open    = FALSE,
                    exit_date  = $1,
                    exit_reason = 'STOP_LOSS'
                WHERE is_open = TRUE
                  AND cin = ANY($2::text[])
                """,
                exit_date,
                exit_cins,
            )
        logger.info("Applied stop-loss exits for %d positions", len(exit_cins))

    critical_actions = [a for a in actions if a.severity == "CRITICAL"]
    if critical_actions:
        await _send_risk_telegram(critical_actions)


async def _send_risk_telegram(actions: list[RiskAction]) -> None:
    """Send CRITICAL risk alerts via existing Telegram deliverer."""
    try:
        import os
        import aiohttp

        token = os.getenv("TELEGRAM_BOT_TOKEN")
        chat_id = os.getenv("TELEGRAM_CHAT_ID")
        if not token or not chat_id:
            logger.warning("Telegram not configured — skipping risk alert")
            return

        lines = ["⚠️ *QUANT RISK ALERT*\n"]
        for action in actions:
            lines.append(f"• *{action.action}*: {action.reason}")

        message = "\n".join(lines)
        url = f"https://api.telegram.org/bot{token}/sendMessage"

        async with aiohttp.ClientSession() as session:
            await session.post(url, json={
                "chat_id": chat_id,
                "text": message,
                "parse_mode": "Markdown",
            })
    except Exception as exc:
        logger.error("Failed to send risk Telegram alert: %s", exc)
