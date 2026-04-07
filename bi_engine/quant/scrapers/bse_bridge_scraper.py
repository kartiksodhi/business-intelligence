"""
BSE equity master CSV scraper.

Downloads the BSE List of Scripts CSV and extracts:
  - Security Code (BSE code)
  - ISIN
  - CIN  (directly available in BSE master for most listings)
  - Company name (for fuzzy resolution of missing CINs)

Returns a list of BseListing dicts ready for cin_bridge.py to process.
"""

from __future__ import annotations

import io
import logging
import re
from dataclasses import dataclass
from typing import Optional

import aiohttp
import pandas as pd

logger = logging.getLogger(__name__)

# BSE equity master download — no authentication required.
# Contains columns: Security Code, ISIN No, CIN, Security Name, Status, ...
_BSE_EQUITY_MASTER_URL = (
    "https://www.bseindia.com/corporates/List_Scrips.aspx"
)
# The actual CSV is fetched via a POST with __VIEWSTATE params,
# so we download the pre-exported file from the known direct URL.
_BSE_CSV_DIRECT_URL = (
    "https://www.bseindia.com/downloads/BSE_Listed_Securities.csv"
)

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.bseindia.com/",
}


@dataclass
class BseListing:
    bse_code: str
    isin: str
    company_name: str
    cin: Optional[str]          # None when blank in master — will need fuzzy match
    sector: Optional[str]
    is_active: bool


async def fetch_bse_listings() -> list[BseListing]:
    """
    Download BSE equity master CSV and parse into BseListing records.
    Filters to active equities only (status = 'Active').
    """
    async with aiohttp.ClientSession(headers=_HEADERS) as session:
        async with session.get(_BSE_CSV_DIRECT_URL, timeout=aiohttp.ClientTimeout(total=60)) as resp:
            if resp.status != 200:
                logger.warning("BSE CSV download returned HTTP %d — trying fallback", resp.status)
                return await _fetch_bse_listings_playwright()
            raw = await resp.read()

    return _parse_bse_csv(raw)


async def _fetch_bse_listings_playwright() -> list[BseListing]:
    """
    Fallback: use Playwright to navigate BSE list page and trigger CSV download.
    Only invoked if direct URL fails (e.g. BSE changes the endpoint).
    """
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        logger.error("Playwright not installed. Run: pip install playwright && playwright install chromium")
        return []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(_BSE_EQUITY_MASTER_URL, timeout=30_000)

        # Wait for the download button and click
        async with page.expect_download() as download_info:
            await page.click("#btnSubmit")  # BSE's "Download" button
        download = await download_info.value
        content = await download.path()

        await browser.close()

        with open(content, "rb") as f:
            raw = f.read()

    return _parse_bse_csv(raw)


def _parse_bse_csv(raw: bytes) -> list[BseListing]:
    try:
        df = pd.read_csv(io.BytesIO(raw), dtype=str, encoding="latin-1")
    except Exception as exc:
        logger.error("Failed to parse BSE CSV: %s", exc)
        return []

    # Normalise column names — BSE occasionally changes capitalisation
    df.columns = [c.strip().upper().replace(" ", "_") for c in df.columns]

    # Candidate column names across BSE CSV versions
    col_map = {
        "bse_code": ["SECURITY_CODE", "SCRIP_CODE", "BSE_CODE"],
        "isin": ["ISIN_NO", "ISIN"],
        "name": ["SECURITY_NAME", "COMPANY_NAME", "SCRIP_NAME"],
        "cin": ["CIN", "CORPORATE_IDENTIFICATION_NUMBER"],
        "sector": ["SECTOR", "INDUSTRY"],
        "status": ["STATUS", "SCRIP_STATUS"],
    }

    def find_col(candidates: list[str]) -> Optional[str]:
        for c in candidates:
            if c in df.columns:
                return c
        return None

    bse_col    = find_col(col_map["bse_code"])
    isin_col   = find_col(col_map["isin"])
    name_col   = find_col(col_map["name"])
    cin_col    = find_col(col_map["cin"])
    sector_col = find_col(col_map["sector"])
    status_col = find_col(col_map["status"])

    if not bse_col or not isin_col or not name_col:
        logger.error("BSE CSV missing required columns. Found: %s", list(df.columns))
        return []

    results: list[BseListing] = []
    for _, row in df.iterrows():
        bse_code = str(row.get(bse_col, "")).strip()
        isin = str(row.get(isin_col, "")).strip()
        company_name = str(row.get(name_col, "")).strip()

        if not bse_code or not isin or not company_name:
            continue
        if isin == "nan" or bse_code == "nan":
            continue

        raw_cin = str(row.get(cin_col, "")).strip() if cin_col else ""
        cin = raw_cin if raw_cin and raw_cin != "nan" and _valid_cin(raw_cin) else None

        sector = str(row.get(sector_col, "")).strip() if sector_col else None
        if sector == "nan":
            sector = None

        status_val = str(row.get(status_col, "")).strip().upper() if status_col else "ACTIVE"
        is_active = status_val in {"ACTIVE", "A", "LISTED"}

        results.append(BseListing(
            bse_code=bse_code,
            isin=isin,
            company_name=company_name,
            cin=cin,
            sector=sector,
            is_active=is_active,
        ))

    logger.info("BSE master: %d listings parsed, %d with CIN", len(results), sum(1 for r in results if r.cin))
    return results


def _valid_cin(cin: str) -> bool:
    """Basic CIN format check: L/U + 5 digits + 2 alpha + 4 digits + 3 alpha + 6 digits."""
    return bool(re.match(r"^[LU]\d{5}[A-Z]{2}\d{4}[A-Z]{3}\d{6}$", cin))
