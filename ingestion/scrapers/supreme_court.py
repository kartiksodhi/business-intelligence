from __future__ import annotations

import logging
import re
from datetime import date, timedelta
from typing import Optional

from playwright.async_api import async_playwright

from .phase3_utils import BaseSignalScraper

logger = logging.getLogger(__name__)

SUPREME_COURT_URL = "https://main.sci.gov.in/php/cl_next.php"


class SupremeCourtScraper(BaseSignalScraper):
    source_id = "supreme_court"
    cadence_hours = 168

    async def run(self) -> list[dict]:
        matters = await self.fetch_recent_matters(date.today() - timedelta(days=7))
        if not matters:
            return []
        seen = set(self._load_state("supreme_court:last_cause_list_date").get("matter_keys", []))
        emitted: list[dict] = []
        for matter in matters:
            key = f"{matter.get('matter_number')}|{matter.get('listing_date')}"
            if key in seen:
                continue
            seen.add(key)
            for company_name in self.extract_company_names(matter.get("party_text") or ""):
                result = self._resolve_entity(company_name)
                if result.cin and result.confidence >= 0.70:
                    event_type, severity = self._classify_matter(matter.get("status_text") or "")
                    payload = {**matter, "company_name": company_name}
                    self._insert_event(result.cin, event_type, severity, payload)
                    emitted.append(payload)
                else:
                    self._store_unmapped(company_name, matter)
        self._store_state(
            "supreme_court:last_cause_list_date",
            {"matter_keys": sorted(seen), "last_cause_list_date": str(date.today())},
            record_count=len(matters),
        )
        return emitted

    async def fetch_recent_matters(self, since: date) -> list[dict]:
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=True)
            page = await browser.new_page()
            response = await page.goto(SUPREME_COURT_URL, wait_until="domcontentloaded", timeout=30000)
            if response and response.status >= 400:
                await browser.close()
                return []
            await page.wait_for_timeout(3000)
            await browser.close()
        return []

    def extract_company_names(self, text: str) -> list[str]:
        parts = re.split(r"\bvs\.?\b|\bversus\b|,|;", text, flags=re.IGNORECASE)
        company_like = []
        for part in parts:
            cleaned = self.normalize_text(part)
            lowered = cleaned.lower()
            if any(token in lowered for token in (" ltd", " limited", " pvt", " llp", " corp", " corporation")):
                company_like.append(cleaned)
        return company_like

    def _classify_matter(self, text: str) -> tuple[str, str]:
        lowered = text.lower()
        if "stay" in lowered:
            return "SC_STAY_GRANTED", "ALERT"
        if "dismissed" in lowered:
            return "SC_APPEAL_DISMISSED", "ALERT"
        return "SC_MATTER_LISTED", "WATCH"

