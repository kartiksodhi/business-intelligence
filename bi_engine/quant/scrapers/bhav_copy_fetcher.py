"""
BSE Bhav Copy fetcher.

Downloads the daily BSE equity Bhav Copy ZIP file and parses OHLCV data.
Called daily at 4:00 PM IST by quant_scheduler.py.

Bhav Copy URL pattern:
  https://www.bseindia.com/download/BhavCopy/Equity/EQ{DDMMYYYY}_CSV.ZIP

The ZIP contains a single CSV: EQ{DDMMYYYY}.CSV with columns:
  SC_CODE, SC_NAME, SC_GROUP, SC_TYPE, OPEN, HIGH, LOW, CLOSE,
  LAST, PREVCLOSE, NO_TRADES, NO_OF_SHRS, NET_TURNOV, ISIN_CODE,
  (sometimes DELIV_QTY, DELIV_PER)
"""

from __future__ import annotations

import io
import logging
import zipfile
from datetime import date, timedelta

import aiohttp
import pandas as pd

logger = logging.getLogger(__name__)

_BHAV_COPY_URL = "https://www.bseindia.com/download/BhavCopy/Equity/EQ{date_str}_CSV.ZIP"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.bseindia.com/",
}


async def fetch_bhav_copy(trade_date: date) -> list[dict]:
    """
    Download and parse BSE Bhav Copy for a given trade date.
    Returns list of dicts with keys matching price_daily columns.
    Returns empty list if market was closed (holiday/weekend).
    """
    date_str = trade_date.strftime("%d%m%Y")
    url = _BHAV_COPY_URL.format(date_str=date_str)

    async with aiohttp.ClientSession(headers=_HEADERS) as session:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=60)) as resp:
            if resp.status == 404:
                logger.debug("Bhav Copy not found for %s (likely holiday)", trade_date)
                return []
            if resp.status != 200:
                logger.error("Bhav Copy download failed for %s: HTTP %d", trade_date, resp.status)
                return []
            raw = await resp.read()

    return _parse_bhav_zip(raw, trade_date)


def _parse_bhav_zip(raw: bytes, trade_date: date) -> list[dict]:
    try:
        with zipfile.ZipFile(io.BytesIO(raw)) as zf:
            csv_name = next((n for n in zf.namelist() if n.endswith(".CSV") or n.endswith(".csv")), None)
            if not csv_name:
                logger.error("No CSV found in Bhav Copy ZIP for %s", trade_date)
                return []
            with zf.open(csv_name) as f:
                df = pd.read_csv(f, dtype=str)
    except Exception as exc:
        logger.error("Failed to parse Bhav Copy ZIP for %s: %s", trade_date, exc)
        return []

    df.columns = [c.strip().upper() for c in df.columns]

    records = []
    for _, row in df.iterrows():
        sc_code = str(row.get("SC_CODE", "")).strip()
        if not sc_code or sc_code == "nan":
            continue

        def _num(col: str) -> float | None:
            v = str(row.get(col, "")).strip()
            try:
                return float(v) if v and v != "nan" else None
            except ValueError:
                return None

        close = _num("CLOSE")
        if close is None:
            continue

        # NET_TURNOV is in rupees — convert to crores
        turnov = _num("NET_TURNOV")
        value_cr = round(turnov / 1e7, 4) if turnov else None

        records.append({
            "ticker_symbol": sc_code,
            "exchange": "BSE",
            "trade_date": trade_date,
            "open_price": _num("OPEN"),
            "high_price": _num("HIGH"),
            "low_price": _num("LOW"),
            "close_price": close,
            "prev_close": _num("PREVCLOSE"),
            "volume": int(float(str(row.get("NO_OF_SHRS", "0")).strip())) if row.get("NO_OF_SHRS") else None,
            "value_cr": value_cr,
            "delivery_qty": int(float(str(row.get("DELIV_QTY", "0")).strip())) if row.get("DELIV_QTY") else None,
            "delivery_pct": _num("DELIV_PER"),
        })

    logger.info("Bhav Copy %s: %d records parsed", trade_date, len(records))
    return records


async def fetch_bhav_copy_range(start: date, end: date) -> list[dict]:
    """
    Fetch Bhav Copy for a date range. Used by backfill_prices.py.
    Skips weekends automatically. Rate-limited: 1 request/second.
    """
    import asyncio

    all_records: list[dict] = []
    current = start
    while current <= end:
        if current.weekday() < 5:  # Mon–Fri
            records = await fetch_bhav_copy(current)
            all_records.extend(records)
            await asyncio.sleep(1.0)
        current += timedelta(days=1)

    return all_records
