from __future__ import annotations

import logging
from datetime import date

from playwright.async_api import async_playwright

from .phase3_utils import BaseSignalScraper

logger = logging.getLogger(__name__)

RERA_PORTALS = {
    "MH": "https://maharerait.mahaonline.gov.in/searchlist/PublicViewDashboard",
    "RJ": "https://rera.rajasthan.gov.in/",
}


class RERAScraper(BaseSignalScraper):
    source_id = "rera"
    cadence_hours = 24 * 30

    async def run(self) -> list[dict]:
        emitted: list[dict] = []
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=True)
            for state_code, url in RERA_PORTALS.items():
                page = await browser.new_page()
                response = await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                if not response or response.status >= 400:
                    logger.warning("rera: %s portal unavailable", state_code)
                    await page.close()
                    continue
                text = (await page.content()).lower()
                if state_code == "MH" and "public" not in text and "dashboard" not in text and "project" not in text:
                    logger.warning("rera: %s structure unclear", state_code)
                self._store_state(f"rera:{state_code}:probe", {"checked_at": str(date.today())}, record_count=0)
                await page.close()
            await browser.close()
        return emitted

    def classify_change(self, previous: dict | None, current: dict) -> tuple[str | None, str | None]:
        if previous and (previous.get("status") != current.get("status")):
            status = (current.get("status") or "").lower()
            if status == "revoked":
                return "RERA_REVOKED", "CRITICAL"
            if status == "lapsed":
                return "RERA_LAPSED", "ALERT"
        if previous and previous.get("complaints_count"):
            old = max(int(previous["complaints_count"]), 1)
            new = int(current.get("complaints_count") or 0)
            if new >= old * 3:
                return "RERA_COMPLAINT_SPIKE", "ALERT"
        if not previous:
            return "RERA_NEW_PROJECT", "INFO"
        return None, None

