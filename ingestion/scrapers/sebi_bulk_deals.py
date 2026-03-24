from __future__ import annotations

import csv
import io
import logging
from datetime import date

from playwright.async_api import async_playwright

from .phase3_utils import BaseSignalScraper

logger = logging.getLogger(__name__)

BSE_BULK_DEALS_URL = "https://www.bseindia.com/markets/equity/EQReports/BulknBlockDeals.aspx"


class SEBIBulkDealsScraper(BaseSignalScraper):
    source_id = "sebi_bulk_deals"
    cadence_hours = 24

    async def run(self) -> list[dict]:
        rows = await self._download_bse_rows()
        if not rows:
            return []
        seen = set(self._load_state("sebi_bulk_deals:last_deal_date").get("deal_keys", []))
        emitted: list[dict] = []
        for row in rows:
            key = f"{row.get('deal_date')}|{row.get('scrip_code')}|{row.get('client_name')}"
            if key in seen:
                continue
            seen.add(key)
            event_type, severity = self.classify_deal(row, promoter_match=False)
            if not event_type:
                continue
            result = self._resolve_entity(row.get("company_name") or "")
            cin = result.cin if result.cin and result.confidence >= 0.70 else None
            if cin:
                self._insert_event(cin, event_type, severity, row)
                emitted.append(row)
            else:
                self._store_unmapped(row.get("company_name") or "", row)
        self._store_state(
            "sebi_bulk_deals:last_deal_date",
            {"deal_keys": sorted(seen), "last_deal_date": str(date.today())},
            record_count=len(rows),
        )
        return emitted

    async def _download_bse_rows(self) -> list[dict]:
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=True)
            page = await browser.new_page()
            await page.goto(BSE_BULK_DEALS_URL, wait_until="domcontentloaded", timeout=30000)
            if not await page.locator("#ContentPlaceHolder1_btnDownload").count():
                await browser.close()
                return []
            async with page.expect_download() as download_info:
                await page.locator("#ContentPlaceHolder1_btnDownload").click()
            download = await download_info.value
            stream = await download.create_read_stream()
            raw = await stream.read() if stream else b""
            await browser.close()
        if not raw:
            return []
        try:
            decoded = raw.decode("utf-8-sig", errors="ignore")
        except Exception:
            return []
        reader = csv.DictReader(io.StringIO(decoded))
        return [self.normalise_row(row) for row in reader]

    def normalise_row(self, row: dict) -> dict:
        return {
            "deal_date": self.parse_date(row.get("Date") or row.get("Deal Date")),
            "scrip_code": row.get("Scrip Code") or row.get("Security Code"),
            "company_name": row.get("Security Name") or row.get("Company Name"),
            "client_name": row.get("Client Name"),
            "deal_type": row.get("Buy/Sell") or row.get("Deal Type"),
            "quantity": self.parse_amount(row.get("Quantity")),
            "price": self.parse_amount(row.get("Price")),
            "deal_value_inr": self.parse_amount(row.get("Value") or row.get("Deal Value")),
        }

    def classify_deal(self, row: dict, promoter_match: bool) -> tuple[str | None, str | None]:
        deal_type = (row.get("deal_type") or "").lower()
        client_name = (row.get("client_name") or "").lower()
        if promoter_match and deal_type == "sell":
            return "SEBI_BULK_DEAL_PROMOTER_SELL", "ALERT"
        if "mutual fund" in client_name or "capital" in client_name:
            return "SEBI_BULK_DEAL_INSTITUTIONAL_EXIT", "WATCH"
        if promoter_match and deal_type == "buy":
            return "SEBI_INSIDER_BUY", "INFO"
        return None, None

