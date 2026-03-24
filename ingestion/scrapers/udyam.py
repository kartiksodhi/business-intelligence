from __future__ import annotations

import logging
import re
from typing import Optional

from playwright.async_api import async_playwright

from .phase3_utils import BaseSignalScraper

logger = logging.getLogger(__name__)

UDYAM_VERIFY_URL = (
    "https://udyamregistration.gov.in/Government-India/Central-Government-org/"
    "udyam-registration-number-verification.aspx"
)

_CLASSIFICATION_RANK = {"micro": 1, "small": 2, "medium": 3}


class UdyamScraper(BaseSignalScraper):
    source_id = "udyam"
    cadence_hours = 24 * 90

    async def run(self) -> list[dict]:
        monitored = self._load_monitored_ids()
        if not monitored:
            return []

        emitted: list[dict] = []
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=True)
            for cin, udyam_id in monitored:
                page = await browser.new_page()
                detail = await self._fetch_status(page, udyam_id)
                await page.close()
                if not detail:
                    continue
                state_key = f"udyam:{udyam_id}"
                previous = self._load_state(state_key)
                self._store_state(state_key, detail)
                event_type, severity = self._classify_transition(previous, detail)
                if event_type:
                    payload = {"cin": cin, **detail, "previous_state": previous}
                    self._insert_event(cin, event_type, severity, payload)
                    emitted.append(payload)
            await browser.close()
        return emitted

    def _load_monitored_ids(self) -> list[tuple[str, str]]:
        try:
            rows = self._fetchall(
                """
                SELECT cin, udyam_id
                FROM master_entities
                WHERE udyam_id IS NOT NULL
                """,
            )
        except Exception as exc:
            logger.warning("udyam: unable to load monitored ids: %s", exc)
            return []

        monitored = []
        for row in rows:
            if isinstance(row, dict):
                cin, udyam_id = row.get("cin"), row.get("udyam_id")
            else:
                cin, udyam_id = row[0], row[1]
            if cin and udyam_id:
                monitored.append((cin, udyam_id))
        return monitored

    async def _fetch_status(self, page, udyam_id: str) -> Optional[dict]:
        response = await page.goto(UDYAM_VERIFY_URL, wait_until="domcontentloaded", timeout=30000)
        if response and response.status >= 400:
            logger.warning("udyam: verification page unavailable status=%s", response.status)
            return None

        number_input = page.locator("input[id*='udyam'], input[name*='udyam']")
        if await number_input.count() == 0:
            logger.warning("udyam: verification selectors unavailable")
            return None
        await number_input.first.fill(udyam_id)

        if not await self._solve_captcha_with_route(
            page,
            "img[id*='captcha'], img[src*='captcha']",
            "input[id*='captcha'], input[name*='captcha']",
        ):
            return None

        submit = page.locator("button[type='submit'], input[type='submit']")
        if await submit.count():
            await submit.first.click()
            await page.wait_for_timeout(1500)

        text = self.normalize_text(await page.locator("body").inner_text())
        if udyam_id.lower() not in text.lower():
            return None

        return {
            "udyam_no": udyam_id,
            "enterprise_name": self._extract_value(text, "enterprise name"),
            "classification": self._extract_classification(text),
            "nic_code": self._extract_value(text, "nic"),
            "registration_date": str(self.parse_date(self._extract_value(text, "date of registration") or "") or ""),
            "status": self._extract_status(text),
        }

    def _extract_value(self, text: str, label: str) -> Optional[str]:
        pattern = re.compile(rf"{re.escape(label)}\s*[:\-]?\s*([A-Za-z0-9/&(),.\- ]+)", re.IGNORECASE)
        match = pattern.search(text)
        return self.normalize_text(match.group(1)) if match else None

    def _extract_classification(self, text: str) -> Optional[str]:
        for label in ("Micro", "Small", "Medium"):
            if label.lower() in text.lower():
                return label
        return None

    def _extract_status(self, text: str) -> Optional[str]:
        lowered = text.lower()
        if "cancelled" in lowered:
            return "Cancelled"
        if "active" in lowered:
            return "Active"
        return None

    def _classify_transition(self, previous: dict, current: dict) -> tuple[Optional[str], Optional[str]]:
        previous_status = (previous.get("status") or "").lower()
        current_status = (current.get("status") or "").lower()
        if previous_status and current_status == "cancelled" and previous_status != current_status:
            return "UDYAM_CANCELLED", "WATCH"

        previous_class = (previous.get("classification") or "").lower()
        current_class = (current.get("classification") or "").lower()
        if previous_class and current_class:
            if _CLASSIFICATION_RANK.get(current_class, 0) > _CLASSIFICATION_RANK.get(previous_class, 0):
                return "UDYAM_CLASSIFICATION_UPGRADE", "INFO"
        if not previous:
            return "UDYAM_NEW", "INFO"
        return None, None

