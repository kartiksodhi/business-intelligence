from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Optional

from playwright.async_api import async_playwright

from .phase3_utils import BaseSignalScraper

logger = logging.getLogger(__name__)

ECOURTS_COMMERCIAL_URL = "https://services.ecourts.gov.in/ecourtindia_v6/"


class HighCourtScraper(BaseSignalScraper):
    source_id = "high_court"
    cadence_hours = 168

    async def run(self) -> list[dict]:
        cases = await self.fetch_recent_cases(date.today() - timedelta(days=7))
        if not cases:
            return []
        emitted: list[dict] = []
        hashes = set(self._load_state("high_court:last_filed_date").get("case_numbers", []))
        for case in cases:
            case_number = case.get("case_number")
            if not case_number or case_number in hashes:
                continue
            hashes.add(case_number)
            cin = self._resolve_case(case)
            if not cin:
                self._store_unmapped(case.get("respondent_name") or "", case)
                continue
            event_type, severity = self._classify_case(case)
            self._insert_event(cin, event_type, severity, case)
            emitted.append(case)
        self._store_state(
            "high_court:last_filed_date",
            {"case_numbers": sorted(hashes), "last_filed_date": str(max((c["filing_date"] for c in cases), default=""))},
            record_count=len(cases),
        )
        return emitted

    async def fetch_recent_cases(self, since: date) -> list[dict]:
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=True)
            page = await browser.new_page()
            response = await page.goto(ECOURTS_COMMERCIAL_URL, wait_until="domcontentloaded", timeout=30000)
            if response and response.status >= 400:
                await browser.close()
                return []
            await browser.close()
        return []

    def _resolve_case(self, case: dict) -> Optional[str]:
        result = self._resolve_entity(case.get("respondent_name") or "")
        if result.cin and result.confidence >= 0.70:
            return result.cin
        return None

    def _classify_case(self, case: dict) -> tuple[str, str]:
        text = f"{case.get('order_type','')} {case.get('court_name','')}".lower()
        if "attachment" in text:
            return "HIGH_COURT_ATTACHMENT", "CRITICAL"
        if "injunction" in text:
            return "HIGH_COURT_INJUNCTION", "ALERT"
        return "HIGH_COURT_COMMERCIAL_SUIT", "ALERT"

    def parse_case(self, cells: list[str]) -> dict:
        return {
            "case_number": cells[0] if len(cells) > 0 else None,
            "filing_date": self.parse_date(cells[1] if len(cells) > 1 else None),
            "petitioner_name": cells[2] if len(cells) > 2 else None,
            "respondent_name": cells[3] if len(cells) > 3 else None,
            "court_name": cells[4] if len(cells) > 4 else None,
            "claim_amount_inr": self.parse_amount(cells[5] if len(cells) > 5 else None),
            "order_type": cells[6] if len(cells) > 6 else None,
        }

