from __future__ import annotations

import logging
from datetime import date
from typing import Optional

from playwright.async_api import async_playwright

from .phase3_utils import BaseSignalScraper

logger = logging.getLogger(__name__)

MCA_CHARGE_URL = "https://www.mca.gov.in/content/mca/global/en/mca/fo-llp-filing/charge.html"


class MCAChargesScraper(BaseSignalScraper):
    source_id = "mca_charges"

    async def run(self, cin: str) -> list[dict]:
        if not cin:
            raise ValueError("cin is required")
        charges = await self.fetch_charges(cin)
        if not charges:
            self._store_state(f"mca_charge:{cin}:last_checked", {"cin": cin, "charges": []}, record_count=0)
            return []

        state_key = f"mca_charge:{cin}:last_checked"
        previous = self._load_state(state_key)
        previous_by_id = {row.get("charge_id"): row for row in previous.get("charges", [])}

        emitted: list[dict] = []
        open_lenders: set[str] = set()
        for charge in charges:
            if charge.get("status") == "Open" and charge.get("lender_name"):
                open_lenders.add(charge["lender_name"])

            event_type, severity = self._classify_charge(previous_by_id.get(charge.get("charge_id")), charge)
            if event_type:
                payload = {"cin": cin, **charge}
                self._insert_event(cin, event_type, severity, payload)
                emitted.append(payload)

        if len(open_lenders) >= 3:
            payload = {"cin": cin, "open_lender_count": len(open_lenders), "charges": charges}
            self._insert_event(cin, "MULTIPLE_LENDERS", "ALERT", payload)
            emitted.append(payload)

        self._store_state(state_key, {"cin": cin, "charges": charges}, record_count=len(charges))
        return charges

    async def fetch_charges(self, cin: str) -> list[dict]:
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=True)
            page = await browser.new_page()
            response = await page.goto(MCA_CHARGE_URL, wait_until="domcontentloaded", timeout=30000)
            if response and response.status >= 400:
                await browser.close()
                return []
            if not await self._solve_captcha_with_route(
                page,
                "img[id*='captcha'], img[src*='captcha']",
                "input[id*='captcha'], input[name*='captcha']",
            ):
                await browser.close()
                return []
            await browser.close()
        return []

    def _classify_charge(self, previous: Optional[dict], current: dict) -> tuple[Optional[str], Optional[str]]:
        amount = current.get("charge_amount_inr") or 0
        if not previous:
            if amount > 100_000_000:
                return "CHARGE_EXCEEDS_CAPITAL", "CRITICAL"
            if amount >= 10_000_000:
                return "CHARGE_CREATED", "ALERT"
            return None, None
        if previous.get("status") != "Satisfied" and current.get("status") == "Satisfied":
            return "CHARGE_SATISFIED", "INFO"
        if current.get("authorized_capital") and amount > current["authorized_capital"]:
            return "CHARGE_EXCEEDS_CAPITAL", "CRITICAL"
        return None, None

    def parse_charge_row(self, cells: list[str], authorized_capital: Optional[int] = None) -> dict:
        return {
            "charge_id": cells[0] if len(cells) > 0 else None,
            "creation_date": self.parse_date(cells[1] if len(cells) > 1 else None),
            "satisfaction_date": self.parse_date(cells[2] if len(cells) > 2 else None),
            "lender_name": cells[3] if len(cells) > 3 else None,
            "charge_amount_inr": self.parse_amount(cells[4] if len(cells) > 4 else None),
            "asset_description": cells[5] if len(cells) > 5 else None,
            "status": cells[6] if len(cells) > 6 else None,
            "authorized_capital": authorized_capital,
        }

