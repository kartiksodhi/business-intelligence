from __future__ import annotations

import logging
import re
from datetime import date, timedelta
from typing import Optional

from playwright.async_api import async_playwright

from .phase3_utils import BaseSignalScraper

logger = logging.getLogger(__name__)

SEBI_ENFORCEMENT_URL = "https://www.sebi.gov.in/enforcement/orders/"


class SEBIEnforcementScraper(BaseSignalScraper):
    source_id = "sebi_enforcement"
    cadence_hours = 168

    async def run(self) -> list[dict]:
        since = date.today() - timedelta(days=7)
        orders = await self._fetch_orders(since)
        if not orders:
            return []

        state = self._load_state("sebi_enforcement:last_order_date")
        seen_hashes = set(state.get("hashes", []))
        emitted: list[dict] = []

        for order in orders:
            order_hash = self.compute_digest(
                [order.get("entity_name"), str(order.get("order_date")), order.get("order_type")]
            )
            if order_hash in seen_hashes:
                continue
            seen_hashes.add(order_hash)

            cin = self._resolve_cin(order.get("entity_name") or "")
            event_type, severity = self._classify_order(order.get("order_type") or "", director=bool(cin is None))
            payload = {**order, "order_hash": order_hash}
            if cin and event_type:
                self._insert_event(cin, event_type, severity, payload)
                emitted.append(payload)
                continue

            director_cin = self._lookup_director_cin(order.get("entity_name") or "")
            if director_cin:
                self._insert_event(director_cin, "SEBI_DIRECTOR_ACTION", "ALERT", payload)
                emitted.append(payload)
            else:
                self._store_unmapped(order.get("entity_name") or "", payload)

        latest = max((order["order_date"] for order in orders if order.get("order_date")), default=None)
        self._store_state(
            "sebi_enforcement:last_order_date",
            {
                "last_order_date": latest.isoformat() if latest else None,
                "hashes": sorted(seen_hashes),
            },
            record_count=len(orders),
        )
        return emitted

    async def _fetch_orders(self, since: date) -> list[dict]:
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=True)
            page = await browser.new_page()
            await page.goto(SEBI_ENFORCEMENT_URL, wait_until="networkidle", timeout=30000)
            rows = await page.query_selector_all("table tr, .result-list li, .media, .views-row")
            orders: list[dict] = []
            for row in rows:
                text = self.normalize_text(await row.inner_text())
                parsed_date = self._extract_date(text)
                if not parsed_date or parsed_date < since:
                    continue
                title_link = await row.query_selector("a")
                title = self.normalize_text(await title_link.inner_text()) if title_link else text
                href = await title_link.get_attribute("href") if title_link else None
                entity_name = self._extract_entity_name(title)
                order_type = self._extract_order_type(text)
                orders.append(
                    {
                        "order_date": parsed_date,
                        "entity_name": entity_name,
                        "order_type": order_type,
                        "order_text_url": href,
                        "title": title,
                    }
                )
            await browser.close()
            return orders

    def _resolve_cin(self, entity_name: str) -> Optional[str]:
        result = self._resolve_entity(entity_name)
        if result.cin and result.confidence >= 0.70:
            return result.cin
        return None

    def _extract_date(self, text: str) -> Optional[date]:
        match = re.search(r"(\d{2}[./-]\d{2}[./-]\d{4}|\d{1,2}\s+[A-Za-z]+\s+\d{4})", text)
        return self.parse_date(match.group(1)) if match else None

    def _extract_entity_name(self, title: str) -> str:
        match = re.search(r"in the matter of\s+(.+)$", title, re.IGNORECASE)
        if match:
            return match.group(1).strip()
        return re.sub(r"^\d{1,2}[-/.]\d{1,2}[-/.]\d{4}\s*", "", title).strip()

    def _extract_order_type(self, text: str) -> str:
        lowered = text.lower()
        if "debar" in lowered or "restraint" in lowered or "restriction" in lowered:
            return "debarment"
        if "investigation" in lowered or "show cause" in lowered:
            return "investigation"
        if "settlement" in lowered:
            return "settlement"
        return "penalty"

    def _classify_order(self, order_type: str, director: bool = False) -> tuple[Optional[str], Optional[str]]:
        lowered = order_type.lower()
        if "debar" in lowered or "restriction" in lowered:
            return "SEBI_DEBARMENT", "CRITICAL"
        if "investigation" in lowered:
            return "SEBI_INVESTIGATION", "WATCH"
        if "settlement" in lowered:
            return "SEBI_PENALTY", "ALERT"
        return "SEBI_PENALTY", "ALERT"
