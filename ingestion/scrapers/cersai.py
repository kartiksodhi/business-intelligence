from __future__ import annotations

import logging
from datetime import date

from playwright.async_api import async_playwright

from .phase3_utils import BaseSignalScraper

logger = logging.getLogger(__name__)

CERSAI_HOME_URL = "https://www.cersai.org.in/CERSAI/home.prg"
CERSAI_BORROWER_URL = "https://www.cersai.org.in/CERSAI/dbtrsrch.prg"


class CERSAIScraper(BaseSignalScraper):
    source_id = "cersai"
    cadence_hours = 24 * 30

    async def run(self) -> list[dict]:
        monitored = self._load_cersai_targets()
        if not monitored:
            return []
        emitted: list[dict] = []
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=True)
            for company in monitored:
                page = await browser.new_page()
                rows = await self._search_company(page, company["cin"])
                await page.close()
                for row in rows:
                    self._insert_event(company["cin"], row["event_type"], row["severity"], row)
                    emitted.append(row)
            await browser.close()
        return emitted

    def _load_cersai_targets(self) -> list[dict]:
        try:
            rows = self._fetchall(
                """
                SELECT cin, company_name
                FROM master_entities
                WHERE health_band IN ('AMBER', 'RED')
                ORDER BY cin
                """
            )
        except Exception:
            return []
        return [
            {"cin": row["cin"], "company_name": row["company_name"]} if isinstance(row, dict) else {"cin": row[0], "company_name": row[1]}
            for row in rows
        ]

    async def _search_company(self, page, cin: str) -> list[dict]:
        response = await page.goto(CERSAI_HOME_URL, wait_until="domcontentloaded", timeout=30000)
        if response and response.status >= 400:
            return []
        await page.goto(CERSAI_BORROWER_URL, wait_until="domcontentloaded", timeout=30000)
        self._store_state(f"cersai:{cin}:last_checked", {"cin": cin, "checked_at": str(date.today())}, record_count=0)
        return []

    def classify_security_interest(self, previous: dict | None, current: dict, open_count: int = 1) -> tuple[str | None, str | None]:
        if previous and previous.get("satisfaction_date") is None and current.get("satisfaction_date"):
            return "CERSAI_SI_SATISFIED", "INFO"
        if open_count >= 3:
            return "CERSAI_MULTIPLE_LENDERS", "ALERT"
        if not previous and (current.get("amount_inr") or 0) >= 10_000_000:
            return "CERSAI_NEW_SI", "ALERT"
        return None, None

