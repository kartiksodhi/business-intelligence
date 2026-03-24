from __future__ import annotations

import logging

from playwright.async_api import async_playwright

from .phase3_utils import BaseSignalScraper

logger = logging.getLogger(__name__)

ESIC_URL = "https://www.esic.gov.in/establishmentsearch"


class ESICScraper(BaseSignalScraper):
    source_id = "esic"
    cadence_hours = 24 * 30

    async def run(self) -> list[dict]:
        monitored = self._load_monitored_ids()
        if not monitored:
            return []
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=True)
            page = await browser.new_page()
            response = await page.goto(ESIC_URL, wait_until="domcontentloaded", timeout=30000)
            await browser.close()
            if response and response.status >= 400:
                return []
        return []

    def _load_monitored_ids(self) -> list[tuple[str, str]]:
        if not self._table_has_column("master_entities", "esic_id"):
            return []
        rows = self._fetchall("SELECT cin, esic_id FROM master_entities WHERE esic_id IS NOT NULL")
        pairs = []
        for row in rows:
            pairs.append((row["cin"], row["esic_id"]) if isinstance(row, dict) else (row[0], row[1]))
        return pairs

    def classify_change(self, previous_status: str | None, current_status: str | None) -> tuple[str | None, str | None]:
        before = (previous_status or "").lower()
        after = (current_status or "").lower()
        if not before and after:
            return "ESIC_NEW", "INFO"
        if after == "cancelled" and before != after:
            return "ESIC_CANCELLED", "ALERT"
        if after == "default":
            return "ESIC_DEFAULT", "WATCH"
        return None, None

