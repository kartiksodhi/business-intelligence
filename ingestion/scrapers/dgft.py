from __future__ import annotations

import logging
import re
from datetime import date, timedelta
from typing import Optional

from playwright.async_api import async_playwright

from .phase3_utils import BaseSignalScraper

logger = logging.getLogger(__name__)

DGFT_ROOT_URL = "https://www.dgft.gov.in/CP/"
DGFT_IEC_URL = "https://www.dgft.gov.in/CP/?opt=iecipr"


class DGFTScraper(BaseSignalScraper):
    source_id = "dgft"
    cadence_hours = 24 * 30

    async def run(self) -> list[dict]:
        since = date.today() - timedelta(days=30)
        rows = await self._fetch_recent_rows(since)
        if not rows:
            return []

        emitted: list[dict] = []
        for row in rows:
            iec_code = row.get("iec_code")
            if not iec_code:
                continue
            state_key = f"dgft:{iec_code}"
            previous = self._load_state(state_key)
            previous_status = (previous.get("status") or "").lower()
            current_status = (row.get("status") or "").lower()
            self._store_state(state_key, row)
            if previous_status == current_status or not previous_status:
                continue

            event_type, severity = self._classify_status(current_status)
            if not event_type:
                continue

            cin = self._lookup_cin_by_column("pan", row.get("pan"))
            if not cin:
                result = self._resolve_entity(row.get("entity_name") or "")
                cin = result.cin if result.cin and result.confidence >= 0.70 else None
            payload = {**row, "previous_status": previous.get("status")}
            if cin:
                self._insert_event(cin, event_type, severity, payload)
                emitted.append(payload)
            else:
                self._store_unmapped(row.get("entity_name") or "", payload)
        return emitted

    async def _fetch_recent_rows(self, since: date) -> list[dict]:
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=True)
            page = await browser.new_page()
            response = await page.goto(DGFT_IEC_URL, wait_until="domcontentloaded", timeout=30000)
            if response and response.status == 404:
                response = await page.goto(DGFT_ROOT_URL, wait_until="domcontentloaded", timeout=30000)
            if response and response.status >= 400:
                logger.warning("dgft: page unavailable status=%s", response.status)
                await browser.close()
                return []

            if not await self._solve_captcha_if_present(page):
                await browser.close()
                return []

            rows = await page.query_selector_all("table tr")
            parsed: list[dict] = []
            for row in rows:
                cells = [self.normalize_text(await cell.inner_text()) for cell in await row.query_selector_all("td")]
                if len(cells) < 3:
                    continue
                cancellation_date = self._extract_date(" ".join(cells))
                if cancellation_date and cancellation_date < since:
                    continue
                parsed.append(
                    {
                        "iec_code": self._extract_iec(cells[0] or " ".join(cells)),
                        "entity_name": cells[1] if len(cells) > 1 else cells[0],
                        "pan": self._extract_pan(" ".join(cells)),
                        "status": self._extract_status(" ".join(cells)),
                        "date_of_cancellation": cancellation_date,
                        "raw_cells": cells,
                    }
                )
            await browser.close()
            return parsed

    async def _solve_captcha_if_present(self, page) -> bool:
        if await page.locator("img[id*='captcha'], img[src*='captcha']").count() == 0:
            return True
        image_selector = "img[id*='captcha'], img[src*='captcha']"
        input_selector = "input[id*='captcha'], input[name*='captcha']"
        return await self._solve_captcha_with_route(page, image_selector, input_selector)

    def _extract_date(self, text: str) -> Optional[date]:
        match = re.search(r"(\d{2}[./-]\d{2}[./-]\d{4}|\d{1,2}\s+[A-Za-z]+\s+\d{4})", text)
        return self.parse_date(match.group(1)) if match else None

    def _extract_pan(self, text: str) -> Optional[str]:
        match = re.search(r"\b[A-Z]{5}\d{4}[A-Z]\b", text)
        return match.group(0) if match else None

    def _extract_iec(self, text: str) -> Optional[str]:
        match = re.search(r"\b\d{10}\b", text)
        return match.group(0) if match else None

    def _extract_status(self, text: str) -> Optional[str]:
        lowered = text.lower()
        if "surrender" in lowered:
            return "Surrendered"
        if "cancel" in lowered:
            return "Cancelled"
        if "active" in lowered:
            return "Active"
        return None

    def _classify_status(self, status: str) -> tuple[Optional[str], Optional[str]]:
        lowered = (status or "").lower()
        if lowered == "cancelled":
            return "IEC_CANCELLED", "ALERT"
        if lowered == "surrendered":
            return "IEC_SURRENDERED", "WATCH"
        if lowered == "active":
            return "IEC_NEW", "INFO"
        return None, None
