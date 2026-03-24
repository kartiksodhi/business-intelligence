from __future__ import annotations

import asyncio
import logging
from typing import Optional

from playwright.async_api import async_playwright

from .phase3_utils import BaseSignalScraper

logger = logging.getLogger(__name__)

NAUKRI_URL = "https://www.naukri.com/"


class NaukriScraper(BaseSignalScraper):
    source_id = "naukri"

    async def run(self) -> list[dict]:
        companies = self._load_watchlist_companies()
        if not companies:
            return []
        return await self._scrape_companies(companies)

    async def _scrape_companies(self, companies: list[dict]) -> list[dict]:
        emitted: list[dict] = []
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=True)
            for company in companies:
                detail = await self.retry_with_backoff(lambda: self._fetch_company(browser, company))
                if detail:
                    emitted.append(detail)
                await asyncio.sleep(2)
            await browser.close()
        return emitted

    async def _fetch_company(self, browser, company: dict) -> Optional[dict]:
        page = await browser.new_page(user_agent="Mozilla/5.0")
        response = await page.goto(NAUKRI_URL, wait_until="domcontentloaded", timeout=30000)
        if not response or response.status >= 400:
            await page.close()
            return None
        text = self.normalize_text(await page.locator("body").inner_text())
        await page.close()
        return {
            "cin": company["cin"],
            "company_name": company["company_name"],
            "job_count": self.parse_amount(text) or 0,
            "role_types": self.extract_role_types(text),
        }

    def extract_role_types(self, text: str) -> list[str]:
        lowered = text.lower()
        roles = []
        for role in ("sales", "finance", "legal", "engineering", "hr", "operations"):
            if role in lowered:
                roles.append(role)
        return roles

