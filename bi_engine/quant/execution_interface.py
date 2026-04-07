"""
Execution interface.

Bridges portfolio_engine decisions to actual order placement.

Mode is controlled by FEATURE_QUANT_LIVE feature flag:
  PAPER (default): log orders to portfolio_positions, no broker API calls.
  LIVE:            place real orders via Zerodha Kite or Upstox API.

LIVE mode requires:
  - ZERODHA_API_KEY + ZERODHA_ACCESS_TOKEN env vars (or UPSTOX_ACCESS_TOKEN)
  - SEBI algo registration via your broker (Zerodha/Upstox handle this automatically
    for API users — no separate filing needed for non-HFT strategies)
  - Gate: FEATURE_QUANT_LIVE=true

IMPORTANT: Paper mode is the default and safe starting point.
Never set FEATURE_QUANT_LIVE=true without operator review.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from datetime import date
from typing import Literal, Optional

import asyncpg

from features import feature
from quant.portfolio_engine import Portfolio
from quant.risk_manager import RiskAction

logger = logging.getLogger(__name__)


@dataclass
class Order:
    ticker_symbol: str
    exchange: str
    direction: Literal["LONG", "SHORT"]
    quantity: int
    order_type: Literal["MARKET", "LIMIT"] = "MARKET"
    limit_price: Optional[float] = None
    product: Literal["CNC", "NRML", "MIS"] = "CNC"  # CNC=delivery, NRML=F&O carry
    status: Literal["PENDING", "PLACED", "FILLED", "REJECTED", "PAPER"] = "PENDING"
    order_id: Optional[str] = None
    fill_price: Optional[float] = None
    notes: str = ""


class ExecutionInterface:
    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool
        self.mode: Literal["PAPER", "LIVE"] = "LIVE" if feature("QUANT_LIVE") else "PAPER"
        logger.info("ExecutionInterface mode: %s", self.mode)

    async def execute_portfolio(self, portfolio: Portfolio, entry_date: date) -> list[Order]:
        """
        Place orders for a new portfolio. PAPER mode: log only.
        LIVE mode: place via broker API.
        """
        orders: list[Order] = []

        for position in portfolio.positions:
            # Determine quantity from weight_pct and portfolio size
            # In paper mode: use notional ₹1Cr portfolio as default
            notional_portfolio = float(os.getenv("QUANT_PORTFOLIO_SIZE_CR", "1")) * 1e7  # ₹1Cr default

            async with self.pool.acquire() as conn:
                from quant.market_data import get_close_price
                price = await get_close_price(conn, position.ticker_symbol, entry_date, position.exchange)

            if not price or price <= 0:
                logger.warning("No price for %s on %s — skipping order", position.ticker_symbol, entry_date)
                continue

            position_value = notional_portfolio * position.weight_pct
            quantity = max(1, int(position_value / price))

            product = "NRML" if position.direction == "SHORT" else "CNC"

            order = Order(
                ticker_symbol=position.ticker_symbol,
                exchange=position.exchange,
                direction=position.direction,
                quantity=quantity,
                order_type="MARKET",
                product=product,
            )

            if self.mode == "PAPER":
                order.status = "PAPER"
                order.fill_price = price
                logger.info(
                    "[PAPER] %s %s %d @ ₹%.2f (weight=%.1f%%)",
                    position.direction, position.ticker_symbol, quantity, price,
                    position.weight_pct * 100,
                )
            else:
                order = await self._place_live_order(order)

            # Update entry_price on the position record
            async with self.pool.acquire() as conn:
                await conn.execute(
                    """
                    UPDATE portfolio_positions
                    SET entry_price = $1, quantity = $2
                    WHERE run_id = $3
                      AND cin = $4
                      AND is_open = TRUE
                    """,
                    order.fill_price or price,
                    quantity,
                    portfolio.run_id,
                    position.cin,
                )

            orders.append(order)

        return orders

    async def process_risk_actions(self, actions: list[RiskAction], exit_date: date) -> None:
        """Process risk manager actions: exit positions, halt entries."""
        for action in actions:
            if action.action == "EXIT_POSITION" and action.ticker_symbol:
                await self._exit_position(action.ticker_symbol, exit_date, action.reason)
            elif action.action == "HALT_ENTRIES":
                logger.critical("[RISK] Halting new entries: %s", action.reason)

    async def _exit_position(self, ticker_symbol: str, exit_date: date, reason: str) -> None:
        """Close a position: place sell/cover order, mark DB closed."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT id, direction, quantity, exchange
                FROM portfolio_positions
                WHERE ticker_symbol = $1 AND is_open = TRUE
                LIMIT 1
                """,
                ticker_symbol,
            )
            if not row:
                return

            if self.mode == "LIVE":
                # Place closing order via broker
                close_direction: Literal["LONG", "SHORT"] = (
                    "SHORT" if row["direction"] == "LONG" else "LONG"
                )
                order = Order(
                    ticker_symbol=ticker_symbol,
                    exchange=row["exchange"],
                    direction=close_direction,
                    quantity=row["quantity"] or 1,
                    order_type="MARKET",
                    product="NRML" if row["direction"] == "SHORT" else "CNC",
                )
                await self._place_live_order(order)
            else:
                logger.info("[PAPER] EXIT %s — reason: %s", ticker_symbol, reason)

            from quant.market_data import get_close_price
            exit_price = await get_close_price(conn, ticker_symbol, exit_date, row["exchange"])

            # Get entry price for P&L
            entry_row = await conn.fetchrow(
                "SELECT entry_price FROM portfolio_positions WHERE id = $1", row["id"]
            )
            entry_price = float(entry_row["entry_price"]) if entry_row and entry_row["entry_price"] else None

            pnl_pct = None
            if entry_price and exit_price and entry_price > 0:
                raw = (exit_price - entry_price) / entry_price
                pnl_pct = -raw if row["direction"] == "SHORT" else raw

            await conn.execute(
                """
                UPDATE portfolio_positions
                SET is_open    = FALSE,
                    exit_date  = $1,
                    exit_price = $2,
                    pnl_pct    = $3,
                    exit_reason = $4
                WHERE id = $5
                """,
                exit_date, exit_price, pnl_pct, reason[:50], row["id"],
            )

    async def _place_live_order(self, order: Order) -> Order:
        """
        Place a real order via Zerodha Kite API.
        Requires ZERODHA_API_KEY and ZERODHA_ACCESS_TOKEN env vars.
        """
        api_key     = os.getenv("ZERODHA_API_KEY")
        access_token = os.getenv("ZERODHA_ACCESS_TOKEN")

        if not api_key or not access_token:
            logger.error("LIVE mode enabled but ZERODHA_API_KEY/ZERODHA_ACCESS_TOKEN not set")
            order.status = "REJECTED"
            order.notes = "Missing broker credentials"
            return order

        try:
            from kiteconnect import KiteConnect  # type: ignore
            kite = KiteConnect(api_key=api_key)
            kite.set_access_token(access_token)

            transaction = kite.TRANSACTION_TYPE_BUY if order.direction == "LONG" else kite.TRANSACTION_TYPE_SELL

            order_id = kite.place_order(
                variety=kite.VARIETY_REGULAR,
                exchange=order.exchange,
                tradingsymbol=order.ticker_symbol,
                transaction_type=transaction,
                quantity=order.quantity,
                product=order.product,
                order_type=kite.ORDER_TYPE_MARKET,
            )
            order.order_id = str(order_id)
            order.status = "PLACED"
            logger.info(
                "[LIVE] Placed %s %s %d — order_id=%s",
                order.direction, order.ticker_symbol, order.quantity, order_id,
            )
        except ImportError:
            logger.error("kiteconnect not installed. Run: pip install kiteconnect")
            order.status = "REJECTED"
            order.notes = "kiteconnect not installed"
        except Exception as exc:
            logger.error("Kite order placement failed for %s: %s", order.ticker_symbol, exc)
            order.status = "REJECTED"
            order.notes = str(exc)[:200]

        return order
