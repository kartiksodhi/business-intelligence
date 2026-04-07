"""
NSE equity list scraper.

Downloads NSE's EQUITY_L.csv which contains: Symbol, Name, ISIN.
We join on ISIN to get NSE symbol for companies already in ticker_bridge via BSE.
Returns a list of NseListing dicts.
"""

from __future__ import annotations

import io
import logging
from dataclasses import dataclass

import aiohttp
import pandas as pd

logger = logging.getLogger(__name__)

_NSE_EQUITY_LIST_URL = (
    "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"
)
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.nseindia.com/",
}


@dataclass
class NseListing:
    nse_symbol: str
    isin: str
    company_name: str
    series: str    # EQ = equity, BE = trade-to-trade, etc.


async def fetch_nse_listings() -> list[NseListing]:
    """
    Download NSE equity list CSV.
    Returns only EQ series (main board equities) by default.
    """
    async with aiohttp.ClientSession(headers=_HEADERS) as session:
        async with session.get(_NSE_EQUITY_LIST_URL, timeout=aiohttp.ClientTimeout(total=30)) as resp:
            if resp.status != 200:
                logger.error("NSE equity list returned HTTP %d", resp.status)
                return []
            raw = await resp.read()

    return _parse_nse_csv(raw)


def _parse_nse_csv(raw: bytes) -> list[NseListing]:
    try:
        df = pd.read_csv(io.BytesIO(raw), dtype=str)
    except Exception as exc:
        logger.error("Failed to parse NSE CSV: %s", exc)
        return []

    df.columns = [c.strip().upper().replace(" ", "_") for c in df.columns]

    symbol_col = next((c for c in ["SYMBOL", "NSE_SYMBOL"] if c in df.columns), None)
    isin_col   = next((c for c in ["ISIN_NUMBER", "ISIN"] if c in df.columns), None)
    name_col   = next((c for c in ["NAME_OF_COMPANY", "COMPANY_NAME"] if c in df.columns), None)
    series_col = next((c for c in ["SERIES"] if c in df.columns), None)

    if not symbol_col or not isin_col:
        logger.error("NSE CSV missing required columns. Found: %s", list(df.columns))
        return []

    results: list[NseListing] = []
    for _, row in df.iterrows():
        symbol = str(row.get(symbol_col, "")).strip()
        isin = str(row.get(isin_col, "")).strip()
        name = str(row.get(name_col, "")).strip() if name_col else ""
        series = str(row.get(series_col, "EQ")).strip() if series_col else "EQ"

        if not symbol or not isin or isin == "nan":
            continue

        results.append(NseListing(
            nse_symbol=symbol,
            isin=isin,
            company_name=name,
            series=series,
        ))

    logger.info("NSE list: %d listings parsed", len(results))
    return results
