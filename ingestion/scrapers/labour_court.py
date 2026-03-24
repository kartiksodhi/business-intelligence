from __future__ import annotations

import logging
from datetime import date, timedelta

from playwright.async_api import async_playwright

from .phase3_utils import BaseSignalScraper

logger = logging.getLogger(__name__)

LABOUR_PORTALS = {
    "MH": "https://lci.gov.in/",
    "DL": "https://delhilabourcourt.nic.in/",
}


class LabourCourtScraper(BaseSignalScraper):
    source_id = "labour_court"
    cadence_hours = 24 * 30

    async def run(self) -> list[dict]:
        emitted: list[dict] = []
        since = date.today() - timedelta(days=30)
        for state_code, url in LABOUR_PORTALS.items():
            batch = await self._fetch_state_orders(state_code, url, since)
            emitted.extend(batch)
        return emitted

    async def _fetch_state_orders(self, state_code: str, url: str, since: date) -> list[dict]:
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=True)
            page = await browser.new_page()
            response = await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            if not response or response.status >= 400:
                logger.warning("labour_court: %s portal unavailable", state_code)
                await browser.close()
                return []
            body = (await page.content()).lower()
            await browser.close()
            if "labour" not in body and "court" not in body:
                logger.warning("labour_court: %s structure unclear", state_code)
                return []
        self._store_state(f"labour_court:{state_code}:last_order_date", {"state": state_code, "checked_at": str(date.today())}, record_count=0)
        return []

    def classify_order(self, order_type: str, employee_count_affected: int | None = None) -> tuple[str, str]:
        lowered = (order_type or "").lower()
        if "retrench" in lowered and (employee_count_affected or 0) > 50:
            return "LABOUR_MASS_RETRENCHMENT", "ALERT"
        if "back" in lowered and "wage" in lowered:
            return "LABOUR_BACK_WAGES", "WATCH"
        return "LABOUR_INDUSTRIAL_DISPUTE", "WATCH"

