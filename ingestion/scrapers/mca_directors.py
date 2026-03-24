from __future__ import annotations

import logging
from typing import Optional

from playwright.async_api import async_playwright

from .phase3_utils import BaseSignalScraper

logger = logging.getLogger(__name__)

MCA_DIRECTOR_URL = "https://www.mca.gov.in/"


class MCADirectorsScraper(BaseSignalScraper):
    source_id = "mca_directors"
    cadence_hours = 24 * 30

    async def run(self, cin: Optional[str] = None) -> list[dict]:
        if not cin:
            return []
        return await self.refresh_cin(cin)

    async def refresh_cin(self, cin: str) -> list[dict]:
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=True)
            page = await browser.new_page()
            await page.goto(MCA_DIRECTOR_URL, wait_until="domcontentloaded", timeout=30000)
            await browser.close()
        self._store_state(f"mca_director:{cin}:last_refresh", {"cin": cin}, record_count=0)
        return []

    def classify_change(self, previous: Optional[dict], current: dict, board_count: int = 0) -> list[tuple[str, str]]:
        events: list[tuple[str, str]] = []
        designation = (current.get("designation") or "").lower()
        if previous and not previous.get("cessation_date") and current.get("cessation_date"):
            if designation == "cfo":
                events.append(("CFO_RESIGNED", "ALERT"))
            else:
                events.append(("DIRECTOR_RESIGNED", "WATCH"))
        if previous and designation == "auditor" and previous.get("director_name") != current.get("director_name"):
            events.append(("AUDITOR_CHANGED", "ALERT"))
        if board_count > 10:
            events.append(("DIRECTOR_OVERLOADED", "WATCH"))
        return events

