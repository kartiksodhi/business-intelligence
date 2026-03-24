from __future__ import annotations

import logging
import re
from datetime import date, timedelta

from playwright.async_api import async_playwright

from .phase3_utils import BaseSignalScraper

logger = logging.getLogger(__name__)

CCI_URLS = [
    "https://www.cci.gov.in/merger-and-acquisitions/orders/summary",
    "https://www.cci.gov.in/antitrust/orders/summary",
]


class CCIScraper(BaseSignalScraper):
    source_id = "cci"
    cadence_hours = 24 * 30

    async def run(self) -> list[dict]:
        since = date.today() - timedelta(days=30)
        orders = await self._fetch_orders(since)
        if not orders:
            return []
        seen = set(self._load_state("cci:last_order_date").get("case_numbers", []))
        emitted: list[dict] = []
        for order in orders:
            case_number = order.get("case_number")
            if not case_number or case_number in seen:
                continue
            seen.add(case_number)
            parties = self.extract_party_names(order.get("party_names") or "")
            resolved_any = False
            event_type, severity = self.classify_order(order.get("order_type") or "")
            for party in parties:
                result = self._resolve_entity(party)
                if result.cin and result.confidence >= 0.70:
                    self._insert_event(result.cin, event_type, severity, {**order, "party_name": party})
                    emitted.append({**order, "party_name": party})
                    resolved_any = True
            if not resolved_any:
                self._store_unmapped(order.get("party_names") or "", order)
        self._store_state("cci:last_order_date", {"case_numbers": sorted(seen), "last_order_date": str(date.today())}, record_count=len(orders))
        return emitted

    async def _fetch_orders(self, since: date) -> list[dict]:
        orders: list[dict] = []
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=True)
            for url in CCI_URLS:
                page = await browser.new_page()
                response = await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                if response and response.status < 400:
                    text = self.normalize_text(await page.locator("body").inner_text())
                    parsed_date = self._extract_date(text)
                    if parsed_date and parsed_date >= since:
                        orders.append(
                            {
                                "order_date": parsed_date,
                                "case_number": self._extract_case_number(text),
                                "party_names": text,
                                "order_type": self._extract_order_type(text),
                                "deal_value_inr": self.parse_amount(text),
                            }
                        )
                await page.close()
            await browser.close()
        return orders

    def _extract_date(self, text: str):
        match = re.search(r"(\d{2}[/-]\d{2}[/-]\d{4}|\d{1,2}\s+[A-Za-z]+\s+\d{4})", text)
        return self.parse_date(match.group(1)) if match else None

    def _extract_case_number(self, text: str) -> str | None:
        match = re.search(r"\b(C[- ]?\d+|Combination\s+Registration\s+No\.\s*[A-Za-z0-9/-]+)\b", text, re.IGNORECASE)
        return self.normalize_text(match.group(0)) if match else None

    def _extract_order_type(self, text: str) -> str:
        lowered = text.lower()
        if "approval" in lowered or "approved" in lowered:
            return "Approval"
        if "penalty" in lowered:
            return "Penalty"
        return "Complaint"

    def extract_party_names(self, text: str) -> list[str]:
        bits = re.split(r"\b(?:and|vs\.?|versus|with)\b", text, flags=re.IGNORECASE)
        return [self.normalize_text(bit) for bit in bits if any(tok in bit.lower() for tok in ("ltd", "limited", "pvt", "llp"))]

    def classify_order(self, order_type: str) -> tuple[str, str]:
        lowered = order_type.lower()
        if "approval" in lowered:
            return "CCI_MERGER_APPROVED", "INFO"
        if "penalty" in lowered:
            return "CCI_PENALTY", "ALERT"
        return "CCI_COMPLAINT", "WATCH"

