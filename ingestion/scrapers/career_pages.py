from __future__ import annotations

import asyncio
import logging
from typing import Optional

from playwright.async_api import async_playwright

from .phase3_utils import BaseSignalScraper

logger = logging.getLogger(__name__)


class CareerPagesScraper(BaseSignalScraper):
    source_id = "career_pages"

    async def run(self) -> list[dict]:
        companies = [company for company in self._load_watchlist_companies(include_careers_url=True) if company.get("careers_url")]
        if not companies:
            return []
        results: list[dict] = []
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=True)
            for company in companies:
                detail = await self.retry_with_backoff(lambda: self._fetch_page(browser, company))
                if detail:
                    results.append(detail)
                await asyncio.sleep(2)
            await browser.close()
        return results

    async def _fetch_page(self, browser, company: dict) -> Optional[dict]:
        page = await browser.new_page(user_agent="Mozilla/5.0")
        response = await page.goto(company["careers_url"], wait_until="domcontentloaded", timeout=30000)
        if not response or response.status >= 400:
            await page.close()
            return None
        text = self.normalize_text(await page.locator("body").inner_text())
        await page.close()
        return {
            "cin": company["cin"],
            "company_name": company["company_name"],
            "careers_url": company["careers_url"],
            "job_count": self.extract_job_count(text),
        }

    def extract_job_count(self, text: str) -> int:
        return self.parse_amount(text) or 0

