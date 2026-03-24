from __future__ import annotations

import logging
from datetime import date, timedelta

from playwright.async_api import async_playwright

from .phase3_utils import BaseSignalScraper

logger = logging.getLogger(__name__)

CPCB_URL = "https://cpcb.gov.in/"


class CPCBScraper(BaseSignalScraper):
    source_id = "cpcb"
    cadence_hours = 24 * 90

    async def run(self) -> list[dict]:
        notices = await self._fetch_notices(date.today() - timedelta(days=90))
        emitted: list[dict] = []
        if not notices:
            return emitted
        seen = set(self._load_state("cpcb:last_notice_date").get("notice_keys", []))
        for notice in notices:
            key = f"{notice.get('notice_date')}|{notice.get('unit_name')}|{notice.get('action_taken')}"
            if key in seen:
                continue
            seen.add(key)
            event_type, severity = self.classify_notice(notice.get("action_taken") or "")
            result = self._resolve_entity(notice.get("unit_name") or "")
            if result.cin and result.confidence >= 0.70:
                self._insert_event(result.cin, event_type, severity, notice)
                emitted.append(notice)
            else:
                self._store_unmapped(notice.get("unit_name") or "", notice)
        self._store_state("cpcb:last_notice_date", {"notice_keys": sorted(seen), "last_notice_date": str(date.today())}, record_count=len(notices))
        return emitted

    async def _fetch_notices(self, since: date) -> list[dict]:
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=True)
            page = await browser.new_page()
            response = await page.goto(CPCB_URL, wait_until="domcontentloaded", timeout=30000)
            if response and response.status >= 400:
                await browser.close()
                return []
            await browser.close()
        return []

    def classify_notice(self, action_taken: str) -> tuple[str, str]:
        lowered = action_taken.lower()
        if "closure" in lowered:
            return "CPCB_CLOSURE_ORDER", "CRITICAL"
        if "revoked" in lowered:
            return "CTO_REVOKED", "CRITICAL"
        return "POLLUTION_NOTICE", "WATCH"

