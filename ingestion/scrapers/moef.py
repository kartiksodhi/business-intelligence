from __future__ import annotations

import logging
import re
from datetime import date, timedelta

from playwright.async_api import async_playwright

from .phase3_utils import BaseSignalScraper

logger = logging.getLogger(__name__)

PARIVESH_URL = "https://cpc.parivesh.nic.in/PV_Search_Proposals.aspx"


class MOEFScraper(BaseSignalScraper):
    source_id = "moef"
    cadence_hours = 24 * 30

    async def run(self) -> list[dict]:
        proposals = await self._fetch_proposals(date.today() - timedelta(days=30))
        emitted: list[dict] = []
        for proposal in proposals:
            state_key = f"moef:{proposal.get('proposal_no')}"
            previous = self._load_state(state_key)
            self._store_state(state_key, proposal)
            event_type, severity = self.classify_change(previous, proposal)
            if not event_type:
                continue
            result = self._resolve_entity(proposal.get("proponent_name") or "")
            if result.cin and result.confidence >= 0.70:
                self._insert_event(result.cin, event_type, severity, proposal)
                emitted.append(proposal)
            else:
                self._store_unmapped(proposal.get("proponent_name") or "", proposal)
        return emitted

    async def _fetch_proposals(self, since: date) -> list[dict]:
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=True)
            page = await browser.new_page()
            response = await page.goto(PARIVESH_URL, wait_until="domcontentloaded", timeout=30000)
            if response and response.status >= 400:
                await browser.close()
                return []
            text = self.normalize_text(await page.locator("body").inner_text())
            await browser.close()
        parsed_date = self._extract_date(text)
        if parsed_date and parsed_date >= since:
            return [
                {
                    "proposal_no": self._extract_proposal_no(text),
                    "project_name": text[:200],
                    "proponent_name": text[:200],
                    "clearance_type": "EC",
                    "status": self._extract_status(text),
                    "project_cost_inr": self.parse_amount(text),
                    "status_change_date": parsed_date,
                }
            ]
        return []

    def _extract_date(self, text: str):
        match = re.search(r"(\d{2}[/-]\d{2}[/-]\d{4}|\d{1,2}\s+[A-Za-z]+\s+\d{4})", text)
        return self.parse_date(match.group(1)) if match else None

    def _extract_proposal_no(self, text: str) -> str | None:
        match = re.search(r"\b[A-Z]{2,}-\d+\b", text)
        return match.group(0) if match else None

    def _extract_status(self, text: str) -> str:
        lowered = text.lower()
        if "revoked" in lowered:
            return "Revoked"
        if "refused" in lowered:
            return "Refused"
        if "granted" in lowered:
            return "Granted"
        return "Applied"

    def classify_change(self, previous: dict, current: dict) -> tuple[str | None, str | None]:
        status = (current.get("status") or "").lower()
        if not previous and (current.get("project_cost_inr") or 0) > 1_000_000_000:
            return "EC_NEW_APPLICATION", "INFO"
        if previous and previous.get("status") == current.get("status"):
            return None, None
        if status == "revoked":
            return "EC_REVOKED", "CRITICAL"
        if status == "refused":
            return "EC_REFUSED", "ALERT"
        if status == "granted":
            return "EC_GRANTED", "INFO"
        return None, None

