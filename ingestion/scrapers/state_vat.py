from __future__ import annotations

import logging

from playwright.async_api import async_playwright

from .phase3_utils import BaseSignalScraper

logger = logging.getLogger(__name__)

STATE_VAT_PORTALS = {
    "MH": "https://mahavat.gov.in/Mahavat/defaulters.jsp",
    "GJ": "https://vat.gujarat.gov.in/",
}


class StateVATScraper(BaseSignalScraper):
    source_id = "state_vat"

    async def run(self) -> list[dict]:
        emitted: list[dict] = []
        for state_code, url in STATE_VAT_PORTALS.items():
            state_key = f"state_vat:{state_code}:backfill_complete"
            previous = self._load_state(state_key)
            if previous.get("backfill_complete"):
                continue
            await self._backfill_state(state_code, url)
            self._store_state(state_key, {"backfill_complete": True, "state": state_code}, record_count=0)
        return emitted

    async def _backfill_state(self, state_code: str, url: str) -> None:
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=True)
            page = await browser.new_page()
            response = await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            if not response or response.status >= 400:
                await browser.close()
                return
            await browser.close()

